"""Text Scouts service (Layer 1B) — a universal event aggregator.

Pipeline::

    sources (GDELT / RSS / official X + Reddit)
        -> normalize  (clean, bound, drop empties)
        -> dedup      (collapse repeats across a rolling window)
        -> relevance  (cheap keyword/impact filter + top-K)
        -> sentiment  (TEXT_SCOUTS workload; local fallback)
        -> publish    kairos.sentiment.signal

Every source is an *official* API/feed (no proxies or self-hosted scrapers), and
each source failure is isolated so one flaky provider never blinds the layer.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime

from kairos_core.bus import build_bus
from kairos_core.contracts import LLMHealthEvent
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics
from kairos_persistence import DurableLLMUsageBudget, DurableMessageBus, SourceStateRepository

from .config import TextSettings
from .dedup import EventDeduplicator
from .filter import LocalRelevanceFilter
from .freshness import EventFreshnessFilter
from .models import NewsItem
from .normalize import EventNormalizer
from .sentiment import SentimentExtractor
from .sources import (
    CommitAwareEventSource,
    EventSource,
    GDELTSource,
    RedditSource,
    RSSSource,
    XApiSource,
)

log = get_logger("text-scouts")


class TextScoutsService:
    def __init__(
        self,
        settings: TextSettings | None = None,
        *,
        gateway=None,
        sources: list[EventSource] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings or TextSettings()
        transport = build_bus(self.settings)
        self.bus = (
            transport
            if self.settings.bus_backend == "memory"
            else DurableMessageBus(transport, service_name=self.settings.service_name)
        )
        self.normalizer = EventNormalizer()
        self.dedup = EventDeduplicator(self.settings.dedup_window_s)
        self.freshness = EventFreshnessFilter(
            self.settings.max_event_age_s,
            self.settings.max_future_skew_s,
            allow_estimated_timestamps=self.settings.allow_estimated_timestamps,
            **({"clock": clock} if clock is not None else {}),
        )
        self.filter = LocalRelevanceFilter(self.settings.relevance_threshold, self.settings.top_k)
        self.sources = sources if sources is not None else self._build_sources()
        if gateway is None:
            from kairos_llm import (
                BudgetedLLMGateway,
                DenyLLMUsageBudget,
                LLMGateway,
                LLMSettings,
                Provider,
            )

            budget = (
                DurableLLMUsageBudget(self.bus)
                if isinstance(self.bus, DurableMessageBus)
                else DenyLLMUsageBudget()
            )
            gateway = BudgetedLLMGateway(
                LLMGateway(settings=LLMSettings(max_retries=0), on_health=self._publish_health),
                budget,
                monthly_budgets_microusd={
                    Provider.OPENAI: 12_000_000,
                    Provider.DEEPSEEK: 1_000_000,
                },
            )
        self.extractor = SentimentExtractor(gateway, source=self.settings.service_name)

    def _build_sources(self) -> list[EventSource]:
        s = self.settings
        sources: list[EventSource] = []
        if s.enable_gdelt:
            sources.append(
                GDELTSource(query=s.gdelt_query, timespan=s.gdelt_timespan, max_records=s.gdelt_max_records)
            )
        if s.enable_rss:
            sources.append(RSSSource(s.rss_feeds))
        if s.enable_x:
            sources.append(
                XApiSource(
                    bearer_token=s.x_bearer_token.get_secret_value(),
                    accounts=s.x_accounts,
                    service_name=s.service_name,
                    max_results=s.x_max_results,
                    max_pages=s.x_max_pages,
                    timeout_s=s.x_timeout_s,
                    monthly_budget_microusd=s.x_monthly_budget_microusd,
                    post_read_unit_cost_microusd=s.x_post_read_unit_cost_microusd,
                    user_read_unit_cost_microusd=s.x_user_read_unit_cost_microusd,
                )
            )
        if s.enable_reddit:
            sources.append(
                RedditSource(
                    client_id=s.reddit_client_id,
                    client_secret=s.reddit_client_secret,
                    user_agent=s.reddit_user_agent,
                    subreddits=s.subreddits,
                    listing=s.reddit_listing,
                    limit=s.reddit_limit,
                )
            )
        return sources

    async def _prepare_sources(self) -> None:
        unbound = [
            source
            for source in self.sources
            if isinstance(source, XApiSource) and source.enabled and not source.state_attached
        ]
        if not unbound:
            return
        if not isinstance(self.bus, DurableMessageBus):
            raise RuntimeError("paid official X polling requires the durable PostgreSQL runtime")
        await self.bus.start()
        repository = SourceStateRepository(self.bus.database.pool)
        for source in unbound:
            source.attach_state(repository)

    async def _gather_with_sources(self) -> tuple[list[NewsItem], list[EventSource]]:
        active = [src for src in self.sources if src.enabled]
        results = await asyncio.gather(*(src.fetch() for src in active), return_exceptions=True)
        items: list[NewsItem] = []
        successful: list[EventSource] = []
        for src, res in zip(active, results, strict=False):
            if isinstance(res, asyncio.CancelledError):
                # Cancellation is lifecycle control, not a recoverable source error.
                raise res
            if isinstance(res, BaseException):
                log.warning("source.error", source=src.name, error=str(res))
                continue
            items.extend(res)
            successful.append(src)
        return items, successful

    async def _gather(self) -> list[NewsItem]:
        items, _successful = await self._gather_with_sources()
        return items

    async def poll_once(self) -> int:
        """Run the pipeline once; returns the number of SentimentSignals published."""
        await self._prepare_sources()
        raw, successful_sources = await self._gather_with_sources()
        commit_aware = [source for source in successful_sources if isinstance(source, CommitAwareEventSource)]
        try:
            timely = self.freshness.select(self.normalizer.normalize(raw))
            candidates = self.dedup.collapse_batch(timely)
            unseen = self.dedup.filter_unseen(candidates)
            relevant = self.filter.select(unseen)
            log.info(
                "text.filtered",
                fetched=len(raw),
                timely=len(timely),
                unique=len(candidates),
                kept=len(relevant),
            )
            published = 0
            signals = await self.extractor.extract(relevant)
            published_refs: set[str] = set()
            for sig in signals:
                await self.bus.publish(Topics.SENTIMENT_SIGNAL, sig)
                log.info("text.signal", topic=sig.topic, sentiment=sig.sentiment, impact=sig.impact.value)
                published_refs.update(sig.sources)
                published += 1
            self.dedup.remember(
                item for item in relevant if published_refs.intersection(item.provenance_refs)
            )
            for source in commit_aware:
                await source.commit_fetch()
            return published
        except BaseException:
            await asyncio.gather(*(source.abort_fetch() for source in commit_aware), return_exceptions=True)
            raise

    async def _publish_health(self, model: str, provider: str, ok: bool, kind: str, latency_s: float) -> None:
        await self.bus.publish(
            Topics.LLM_HEALTH,
            LLMHealthEvent(
                source=self.settings.service_name,
                provider=provider,
                model=model,
                ok=ok,
                kind=kind,
                latency_s=latency_s,
            ),
        )

    async def run(self) -> None:  # pragma: no cover - network
        configure_logging(
            self.settings.log_level, json_logs=self.settings.log_json, service=self.settings.service_name
        )
        log.info("text.start", sources=[s.name for s in self.sources if s.enabled])
        while True:
            await self.poll_once()
            await asyncio.sleep(self.settings.poll_interval_s)


def main() -> None:  # pragma: no cover
    asyncio.run(TextScoutsService().run())


if __name__ == "__main__":
    main()

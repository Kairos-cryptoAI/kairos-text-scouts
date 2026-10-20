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
import hashlib
from collections.abc import Callable
from datetime import datetime
from typing import Protocol
from uuid import uuid4

from kairos_core.bus import MessageBus, build_bus
from kairos_core.contracts import LLMHealthEvent
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics
from kairos_persistence import (
    QUALIFICATION_CAMPAIGN_ID,
    DurableLLMUsageBudget,
    DurableMessageBus,
    SourceStateRepository,
)

from .config import TextSettings
from .dedup import EventDeduplicator, dedup_key
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


class _NewsProcessingRepository(Protocol):
    async def claim_batch(
        self, scope_key: str, event_keys: tuple[str, ...], attempt_id: str
    ) -> tuple[str, ...]: ...

    async def complete_batch(self, scope_key: str, event_keys: tuple[str, ...], attempt_id: str) -> None: ...


class TextScoutsService:
    def __init__(
        self,
        settings: TextSettings | None = None,
        *,
        gateway=None,
        bus: MessageBus | None = None,
        sources: list[EventSource] | None = None,
        clock: Callable[[], datetime] | None = None,
        news_processing_repository: _NewsProcessingRepository | None = None,
    ) -> None:
        self.settings = settings or TextSettings()
        if bus is not None:
            self.bus = bus
        else:
            transport = build_bus(self.settings)
            self.bus = (
                transport
                if self.settings.bus_backend == "memory"
                else DurableMessageBus(transport, service_name=self.settings.service_name)
            )
        self.normalizer = EventNormalizer()
        self.dedup = EventDeduplicator(self.settings.dedup_window_s)
        self._news_processing_repository = news_processing_repository
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
        repository = SourceStateRepository(self.bus.database.pool, campaign_id=QUALIFICATION_CAMPAIGN_ID)
        for source in unbound:
            source.attach_state(repository)

    def _durable_processing_scope(self) -> str:
        """Validate the durable scope/profile before any connection or source work."""
        if not isinstance(self.bus, DurableMessageBus):
            raise RuntimeError("durable news processing requires DurableMessageBus")
        if self.bus.database.migration_profile.value != "controlled-runtime":
            raise RuntimeError("durable news processing requires the controlled-runtime database profile")
        scope_key = self.settings.processing_scope_sha256
        if len(scope_key) != 64 or any(c not in "0123456789abcdef" for c in scope_key):
            raise RuntimeError("durable news processing requires processing_scope_sha256")

        return scope_key

    def _durable_processing_repository(self) -> _NewsProcessingRepository:
        """Resolve the durable repository after the database bus has started."""
        if not isinstance(self.bus, DurableMessageBus) or not self.bus._started:
            raise RuntimeError("durable news repository requires a started DurableMessageBus")
        repository = self._news_processing_repository
        if repository is None:
            import kairos_persistence

            repository_type = getattr(kairos_persistence, "NewsProcessingRepository", None)
            if repository_type is None:
                raise RuntimeError(
                    "installed kairos-persistence lacks NewsProcessingRepository; upgrade the pinned runtime"
                )
            repository = repository_type(self.bus.database.pool)
            self._news_processing_repository = repository
        return repository

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
        durable_processing: tuple[str, _NewsProcessingRepository] | None = None
        if self.settings.bus_backend != "memory" and not isinstance(self.bus, DurableMessageBus):
            raise RuntimeError("non-memory Text Scouts requires DurableMessageBus")
        if isinstance(self.bus, DurableMessageBus):
            # Check the profile before _prepare_sources can connect and invoke DDL.
            scope_key = self._durable_processing_scope()
            await self.bus.start()
            durable_processing = (scope_key, self._durable_processing_repository())
        await self._prepare_sources()
        raw, successful_sources = await self._gather_with_sources()
        commit_aware = [source for source in successful_sources if isinstance(source, CommitAwareEventSource)]
        try:
            timely = self.freshness.select(self.normalizer.normalize(raw))
            candidates = self.dedup.collapse_batch(timely)
            unseen = self.dedup.filter_unseen(candidates)
            relevant = self.filter.select(unseen)
            claimed_keys: tuple[str, ...] = ()
            attempt_id: str | None = None
            if durable_processing is not None:
                scope_key, repository = durable_processing
                key_to_item = {
                    hashlib.sha256(dedup_key(item).encode("utf-8")).hexdigest(): item for item in relevant
                }
                requested_keys = tuple(sorted(key_to_item))
                if requested_keys:
                    attempt_id = hashlib.sha256(uuid4().bytes).hexdigest()
                    claimed = await repository.claim_batch(scope_key, requested_keys, attempt_id)
                    if (
                        not isinstance(claimed, tuple)
                        or any(not isinstance(key, str) or key not in key_to_item for key in claimed)
                        or len(set(claimed)) != len(claimed)
                    ):
                        raise RuntimeError("news processing repository returned an invalid claim set")
                    claimed_keys = tuple(sorted(claimed))
                    relevant = [key_to_item[key] for key in claimed_keys]
                    if len(requested_keys) > len(claimed_keys):
                        log.info(
                            "text.durable_dedup",
                            claimed=len(claimed_keys),
                            previously_fenced=len(requested_keys) - len(claimed_keys),
                        )
            log.info(
                "text.filtered",
                fetched=len(raw),
                timely=len(timely),
                unique=len(candidates),
                kept=len(relevant),
            )
            published = 0
            # Do not call (or reserve budget for) the provider when every event
            # was already fenced by durable processing state.
            signals = await self.extractor.extract(relevant) if relevant else []
            published_refs: set[str] = set()
            for sig in signals:
                await self.bus.publish(Topics.SENTIMENT_SIGNAL, sig)
                log.info("text.signal", topic=sig.topic, sentiment=sig.sentiment, impact=sig.impact.value)
                published_refs.update(sig.sources)
                published += 1
            if durable_processing is not None and claimed_keys:
                if attempt_id is None:
                    raise RuntimeError("durable news claim is missing its attempt identity")
                await durable_processing[1].complete_batch(durable_processing[0], claimed_keys, attempt_id)
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

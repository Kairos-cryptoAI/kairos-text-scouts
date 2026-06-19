"""Text Scouts service (Layer 1B) — a universal event aggregator.

Pipeline::

    sources (GDELT / RSS / Bright Data X+Reddit)
        -> normalize  (clean, bound, drop empties)
        -> dedup      (collapse repeats across a rolling window)
        -> relevance  (cheap keyword/impact filter + top-K)
        -> sentiment  (DeepSeek-V4-Flash, non-thinking; local fallback)
        -> publish    kairos.sentiment.signal

Every source is an *official* API/feed (no proxies or self-hosted scrapers), and
each source failure is isolated so one flaky provider never blinds the layer.
"""
from __future__ import annotations

import asyncio
from typing import List

from kairos_core.bus import build_bus
from kairos_core.contracts import LLMHealthEvent
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .config import TextSettings
from .dedup import EventDeduplicator
from .filter import LocalRelevanceFilter
from .models import NewsItem
from .normalize import EventNormalizer
from .sentiment import SentimentExtractor
from .sources import BrightDataRedditSource, BrightDataXSource, EventSource, GDELTSource, RSSSource

log = get_logger("text-scouts")


class TextScoutsService:
    def __init__(self, settings: TextSettings | None = None, *, gateway=None,
                 sources: List[EventSource] | None = None) -> None:
        self.settings = settings or TextSettings()
        self.bus = build_bus(self.settings)
        self.normalizer = EventNormalizer()
        self.dedup = EventDeduplicator(self.settings.dedup_window_s)
        self.filter = LocalRelevanceFilter(self.settings.relevance_threshold, self.settings.top_k)
        self.sources = sources if sources is not None else self._build_sources()
        if gateway is None:
            from kairos_llm import LLMGateway  # lazy: only needed at runtime
            gateway = LLMGateway(on_health=self._publish_health)
        self.extractor = SentimentExtractor(gateway, source=self.settings.service_name)

    def _build_sources(self) -> List[EventSource]:
        s = self.settings
        sources: List[EventSource] = []
        if s.enable_gdelt:
            sources.append(GDELTSource(query=s.gdelt_query, timespan=s.gdelt_timespan,
                                       max_records=s.gdelt_max_records))
        if s.enable_rss:
            sources.append(RSSSource(s.rss_feeds))
        if s.enable_x:
            sources.append(BrightDataXSource(token=s.brightdata_api_token,
                                             dataset_id=s.brightdata_x_dataset_id,
                                             accounts=s.x_accounts, num_posts=s.x_num_posts,
                                             poll_timeout_s=s.brightdata_poll_timeout_s))
        if s.enable_reddit:
            sources.append(BrightDataRedditSource(token=s.brightdata_api_token,
                                                  dataset_id=s.brightdata_reddit_dataset_id,
                                                  subreddits=s.subreddits,
                                                  poll_timeout_s=s.brightdata_poll_timeout_s))
        return sources

    async def _gather(self) -> List[NewsItem]:
        active = [src for src in self.sources if src.enabled]
        results = await asyncio.gather(*(src.fetch() for src in active), return_exceptions=True)
        items: List[NewsItem] = []
        for src, res in zip(active, results, strict=False):
            if isinstance(res, Exception):
                log.warning("source.error", source=src.name, error=str(res))
                continue
            items.extend(res)
        return items

    async def poll_once(self) -> int:
        """Run the pipeline once; returns the number of SentimentSignals published."""
        raw = await self._gather()
        fresh = self.dedup.filter_new(self.normalizer.normalize(raw))
        relevant = self.filter.select(fresh)
        log.info("text.filtered", fetched=len(raw), fresh=len(fresh), kept=len(relevant))
        published = 0
        for sig in await self.extractor.extract(relevant):
            await self.bus.publish(Topics.SENTIMENT_SIGNAL, sig)
            log.info("text.signal", topic=sig.topic, sentiment=sig.sentiment, impact=sig.impact.value)
            published += 1
        return published

    async def _publish_health(self, model: str, provider: str, ok: bool, kind: str, latency_s: float) -> None:
        await self.bus.publish(Topics.LLM_HEALTH, LLMHealthEvent(
            source=self.settings.service_name, provider=provider, model=model,
            ok=ok, kind=kind, latency_s=latency_s))

    async def run(self) -> None:  # pragma: no cover - network
        configure_logging(self.settings.log_level, json_logs=self.settings.log_json,
                          service=self.settings.service_name)
        log.info("text.start", sources=[s.name for s in self.sources if s.enabled])
        while True:
            await self.poll_once()
            await asyncio.sleep(self.settings.poll_interval_s)


def main() -> None:  # pragma: no cover
    asyncio.run(TextScoutsService().run())


if __name__ == "__main__":
    main()

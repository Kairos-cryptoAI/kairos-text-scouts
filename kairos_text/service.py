"""Text Scouts service: sources -> local filter -> LLM sentiment -> bus."""
from __future__ import annotations

import asyncio

from kairos_core.bus import build_bus
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .config import TextSettings
from .filter import LocalRelevanceFilter
from .sentiment import SentimentExtractor
from .sources import RSSSource

log = get_logger("text-scouts")


class TextScoutsService:
    def __init__(self, settings: TextSettings | None = None, *, gateway=None) -> None:
        self.settings = settings or TextSettings()
        self.bus = build_bus(self.settings)
        self.filter = LocalRelevanceFilter(self.settings.relevance_threshold, self.settings.top_k)
        self.source = RSSSource(self.settings.rss_feeds)
        if gateway is None:
            from kairos_llm import LLMGateway  # lazy: only needed at runtime
            gateway = LLMGateway()
        self.extractor = SentimentExtractor(gateway, source=self.settings.service_name)

    async def run(self) -> None:  # pragma: no cover - network
        configure_logging(self.settings.log_level, json_logs=self.settings.log_json, service=self.settings.service_name)
        log.info("text.start", feeds=len(self.settings.rss_feeds))
        while True:
            raw = await self.source.fetch()
            relevant = self.filter.select(raw)
            log.info("text.filtered", fetched=len(raw), kept=len(relevant))
            for sig in await self.extractor.extract(relevant):
                await self.bus.publish(Topics.SENTIMENT_SIGNAL, sig)
                log.info("text.signal", topic=sig.topic, sentiment=sig.sentiment, impact=sig.impact.value)
            await asyncio.sleep(self.settings.poll_interval_s)


def main() -> None:  # pragma: no cover
    asyncio.run(TextScoutsService().run())


if __name__ == "__main__":
    main()

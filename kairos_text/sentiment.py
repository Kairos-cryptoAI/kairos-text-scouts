"""Turn a batch of relevant items into SentimentSignal messages via the LLM gateway.

The LLM call uses the explicit ``TEXT_SCOUTS`` workload. ``kairos-llm`` owns its
provider/model/reasoning route. If that route is unavailable the extractor degrades
to a deterministic local fallback (see :mod:`kairos_text.local`) so the Router keeps
receiving a coarse text bias instead of going blind.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

from kairos_core.contracts import SentimentSignal
from kairos_llm import LLMWorkload

from .local import local_sentiment
from .models import NewsItem
from .prompts import SENTIMENT_SYSTEM
from .schemas import SentimentBatch


class SentimentExtractor:
    """Wraps an ``LLMGateway`` (duck-typed) with an async ``complete`` coroutine."""

    def __init__(self, gateway, *, source: str = "text-scouts") -> None:
        self.gateway = gateway
        self.source = source

    def _format_batch(self, items: Sequence[NewsItem]) -> str:
        payload = {
            "items": [
                {
                    "id": index,
                    "source": item.source,
                    "source_kind": item.source_kind,
                    "title": item.title,
                    "body": item.body,
                    "url": item.url,
                    "published_at": item.published_at.isoformat(),
                }
                for index, item in enumerate(items, start=1)
            ]
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    async def extract(self, items: Sequence[NewsItem]) -> list[SentimentSignal]:
        if not items:
            return []
        try:
            res = await self.gateway.complete(
                system=SENTIMENT_SYSTEM,
                user=self._format_batch(items),
                workload=LLMWorkload.TEXT_SCOUTS,
                schema=SentimentBatch,
            )
            batch = (
                res.parsed
                if isinstance(res.parsed, SentimentBatch)
                else SentimentBatch.model_validate(res.parsed)
            )
        except Exception:
            # DeepSeek-V4-Flash unavailable -> degrade to local filtering mode.
            return local_sentiment(items, source=f"{self.source}:local")

        indexed_items = {index: item for index, item in enumerate(items, start=1)}
        signals: list[SentimentSignal] = []
        for extracted in batch.signals:
            evidence = [indexed_items[item_id] for item_id in extracted.item_ids if item_id in indexed_items]
            if not evidence:
                continue
            sources = list(
                dict.fromkeys(item.url or item.source for item in evidence if item.url or item.source)
            )
            signals.append(
                SentimentSignal(
                    source=self.source,
                    topic=extracted.topic,
                    sentiment=extracted.sentiment,
                    impact=extracted.impact,
                    confidence=extracted.confidence,
                    sources=sources,
                    summary=extracted.summary,
                )
            )
        return signals

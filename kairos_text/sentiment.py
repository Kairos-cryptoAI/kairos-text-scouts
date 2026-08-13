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
from .signals import build_signal

_MIN_MODEL_CONFIDENCE = 0.35
_MIN_DIRECTIONAL_SENTIMENT = 0.1


def _confidence_cap(evidence_count: int) -> float:
    """Calibrate model self-confidence to the amount of independent evidence."""
    return min(0.9, 0.65 + 0.1 * (evidence_count - 1))


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
            item_ids = list(dict.fromkeys(extracted.item_ids))
            if len(item_ids) != len(extracted.item_ids) or any(
                item_id not in indexed_items for item_id in item_ids
            ):
                continue
            evidence = [indexed_items[item_id] for item_id in item_ids]
            sources = sorted({ref for item in evidence for ref in item.provenance_refs}, key=str.casefold)
            if not sources or extracted.confidence < _MIN_MODEL_CONFIDENCE:
                continue
            if (extracted.sentiment > 0 and extracted.impact.value != "bullish") or (
                extracted.sentiment < 0 and extracted.impact.value != "bearish"
            ):
                continue
            if abs(extracted.sentiment) < _MIN_DIRECTIONAL_SENTIMENT or extracted.impact.value == "neutral":
                continue
            signals.append(
                build_signal(
                    source=self.source,
                    topic=extracted.topic,
                    sentiment=extracted.sentiment,
                    impact=extracted.impact,
                    confidence=min(extracted.confidence, _confidence_cap(len(sources))),
                    sources=sources,
                    summary=extracted.summary,
                    evidence=evidence,
                )
            )
        signals.sort(
            key=lambda signal: (
                signal.topic.casefold(),
                tuple(ref.casefold() for ref in signal.sources),
                signal.impact.value,
                signal.sentiment,
            )
        )
        return signals

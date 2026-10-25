"""Turn a batch of relevant items into SentimentSignal messages via the LLM gateway.

The LLM call uses the explicit ``TEXT_SCOUTS`` workload. ``kairos-llm`` owns its
provider/model/reasoning route. If that route is unavailable the extractor degrades
to a deterministic local fallback (see :mod:`kairos_text.local`) so the Router keeps
receiving a coarse text bias instead of going blind.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import replace

from kairos_core.contracts import SentimentSignal
from kairos_llm import LLMWorkload

from .local import local_sentiment
from .models import NewsItem
from .native_capture import (
    NativeTextCaptureSink,
    emit_sentiment_extraction,
    preflight_sentiment_capture,
    utc_clock_ms,
)
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

    def __init__(
        self,
        gateway,
        *,
        source: str = "text-scouts",
        capture_sink: NativeTextCaptureSink | None = None,
        clock_ms: Callable[[], int] = utc_clock_ms,
    ) -> None:
        self.gateway = gateway
        self.source = source
        self.capture_sink = capture_sink
        self._clock_ms = clock_ms

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
        items = tuple(replace(item) for item in items)
        user = self._format_batch(items)
        if self.capture_sink is not None:
            preflight_sentiment_capture(
                items, system=SENTIMENT_SYSTEM, user=user, schema=SentimentBatch.model_json_schema()
            )
        try:
            res = await self.gateway.complete(
                system=SENTIMENT_SYSTEM,
                user=user,
                workload=LLMWorkload.TEXT_SCOUTS,
                schema=SentimentBatch,
            )
            batch = (
                res.parsed
                if isinstance(res.parsed, SentimentBatch)
                else SentimentBatch.model_validate(res.parsed)
            )
        except Exception:
            # The selected provider route is unavailable: retain only the coarse local fallback.
            fallback = local_sentiment(items, source=f"{self.source}:local")
            if self.capture_sink is not None:
                emit_sentiment_extraction(
                    self.capture_sink,
                    extractor_source=self.source,
                    items=items,
                    signals=fallback,
                    system=SENTIMENT_SYSTEM,
                    user=user,
                    schema=SentimentBatch.model_json_schema(),
                    result=None,
                    captured_at_ms=self._clock_ms(),
                    fallback_reason_code="PROVIDER_OR_SCHEMA_FAILURE",
                )
            return fallback

        signals = self._signals_from_batch(items, batch)
        if self.capture_sink is not None:
            emit_sentiment_extraction(
                self.capture_sink,
                extractor_source=self.source,
                items=items,
                signals=signals,
                system=SENTIMENT_SYSTEM,
                user=user,
                schema=SentimentBatch.model_json_schema(),
                result=res,
                captured_at_ms=self._clock_ms(),
            )
        return signals

    def _signals_from_batch(self, items: Sequence[NewsItem], batch: SentimentBatch) -> list[SentimentSignal]:
        """Native deterministic model-output transform, also usable by evidence replay."""
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

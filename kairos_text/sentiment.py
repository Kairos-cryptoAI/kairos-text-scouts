"""Turn a batch of relevant items into SentimentSignal messages via the LLM gateway.

The LLM call runs at ``ReasoningEffort.LOW``, which kairos-llm routes to
DeepSeek-V4-Flash in non-thinking mode. If that model is unavailable the extractor
degrades to a deterministic local fallback (see :mod:`kairos_text.local`) so the
Router keeps receiving a coarse text bias instead of going blind.
"""
from __future__ import annotations

from typing import List, Sequence

from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection, ReasoningEffort

from .local import local_sentiment
from .models import NewsItem
from .prompts import SENTIMENT_SYSTEM


class SentimentExtractor:
    """Wraps an ``LLMGateway`` (duck-typed) with an async ``complete`` coroutine."""

    def __init__(self, gateway, *, source: str = "text-scouts") -> None:
        self.gateway = gateway
        self.source = source

    def _format_batch(self, items: Sequence[NewsItem]) -> str:
        lines = [f"{i+1}. [{it.source}] {it.title}" for i, it in enumerate(items)]
        return "\n".join(lines)

    async def extract(self, items: Sequence[NewsItem]) -> List[SentimentSignal]:
        if not items:
            return []
        try:
            res = await self.gateway.complete(
                system=SENTIMENT_SYSTEM, user=self._format_batch(items), effort=ReasoningEffort.LOW
            )
        except Exception:
            # DeepSeek-V4-Flash unavailable -> degrade to local filtering mode.
            return local_sentiment(items, source=f"{self.source}:local")
        data = res.parsed if isinstance(res.parsed, dict) else {}
        signals: List[SentimentSignal] = []
        for raw in data.get("signals", []):
            try:
                signals.append(SentimentSignal(
                    source=self.source,
                    topic=raw["topic"],
                    sentiment=float(raw["sentiment"]),
                    impact=ImpactDirection(str(raw.get("impact", "neutral")).lower()),
                    confidence=float(raw.get("confidence", 0.5)),
                    summary=raw.get("summary", ""),
                ))
            except (KeyError, ValueError):
                continue
        return signals

"""Turn a batch of relevant items into SentimentSignal messages via the LLM gateway."""
from __future__ import annotations

from typing import List, Sequence

from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection, ReasoningEffort

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
        res = await self.gateway.complete(
            system=SENTIMENT_SYSTEM, user=self._format_batch(items), effort=ReasoningEffort.LOW
        )
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

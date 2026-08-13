"""Degraded, LLM-free sentiment used when DeepSeek-V4-Flash is unavailable.

Circuit Breaker fallback (see the architecture document): if the Flash model is
down, Text Scouts drop to *local filtering* — a deterministic keyword scorer that
still emits coarse, low-confidence SentimentSignals so the Router keeps receiving
a text bias instead of going blind.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection

from .models import NewsItem
from .signals import build_signal

BULLISH_TERMS = {
    "surge",
    "soar",
    "rally",
    "approve",
    "approval",
    "record",
    "adopt",
    "adoption",
    "bull",
    "gain",
    "inflow",
    "upgrade",
}
BEARISH_TERMS = {
    "plunge",
    "crash",
    "ban",
    "reject",
    "breach",
    "hack",
    "lawsuit",
    "warning",
    "outflow",
    "bear",
    "selloff",
    "exploit",
    "default",
}
_TOKEN = re.compile(r"[a-z0-9]+")


def _directional_hits(text: str) -> tuple[int, int]:
    tokens = set(_TOKEN.findall(text.lower()))
    return (
        sum(1 for word in BULLISH_TERMS if word in tokens),
        sum(1 for word in BEARISH_TERMS if word in tokens),
    )


def score_text(text: str) -> float:
    """Net keyword sentiment in [-1, 1]."""
    pos, neg = _directional_hits(text)
    if pos == neg:
        return 0.0
    return max(-1.0, min(1.0, (pos - neg) / float(pos + neg)))


def local_sentiment(items: Sequence[NewsItem], *, source: str = "text-scouts:local") -> list[SentimentSignal]:
    """Emit only directional, attributable evidence in degraded mode.

    Neutral or contradictory keyword matches are abstentions. Publishing a neutral
    placeholder would be indistinguishable downstream from actual neutral evidence.
    """
    signals: list[SentimentSignal] = []
    for it in items:
        refs = list(it.provenance_refs)
        positive_hits, negative_hits = _directional_hits(it.text)
        s = score_text(it.text)
        if s == 0.0 or (positive_hits and negative_hits) or not refs:
            continue
        impact = (
            ImpactDirection.BULLISH
            if s > 0
            else ImpactDirection.BEARISH
            if s < 0
            else ImpactDirection.NEUTRAL
        )
        signals.append(
            build_signal(
                source=source,
                topic=(it.title[:48] or "news"),
                sentiment=s,
                impact=impact,
                confidence=min(0.25, 0.1 + 0.05 * (positive_hits + negative_hits)),
                sources=refs,
                summary="local keyword fallback (Flash unavailable)",
                evidence=[it],
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

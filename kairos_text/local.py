"""Degraded, LLM-free sentiment used when DeepSeek-V4-Flash is unavailable.

Circuit Breaker fallback (see the architecture document): if the Flash model is
down, Text Scouts drop to *local filtering* — a deterministic keyword scorer that
still emits coarse, low-confidence SentimentSignals so the Router keeps receiving
a text bias instead of going blind.
"""
from __future__ import annotations

from typing import List, Sequence

from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection

from .models import NewsItem

BULLISH_TERMS = {"surge", "soar", "rally", "approve", "approval", "record",
                 "adopt", "adoption", "bull", "gain", "inflow", "upgrade"}
BEARISH_TERMS = {"plunge", "crash", "ban", "reject", "breach", "hack", "lawsuit",
                 "warning", "outflow", "bear", "selloff", "exploit", "default"}


def score_text(text: str) -> float:
    """Net keyword sentiment in [-1, 1]."""
    t = text.lower()
    pos = sum(1 for w in BULLISH_TERMS if w in t)
    neg = sum(1 for w in BEARISH_TERMS if w in t)
    if pos == neg:
        return 0.0
    return max(-1.0, min(1.0, (pos - neg) / float(pos + neg)))


def local_sentiment(items: Sequence[NewsItem], *, source: str = "text-scouts:local") -> List[SentimentSignal]:
    signals: List[SentimentSignal] = []
    for it in items:
        s = score_text(it.text)
        impact = (ImpactDirection.BULLISH if s > 0 else
                  ImpactDirection.BEARISH if s < 0 else ImpactDirection.NEUTRAL)
        signals.append(SentimentSignal(
            source=source,
            topic=(it.title[:48] or "news"),
            sentiment=s,
            impact=impact,
            confidence=0.2,  # degraded mode: deliberately low confidence
            summary="local keyword fallback (Flash unavailable)",
        ))
    return signals

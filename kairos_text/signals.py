"""Replay-stable construction of bus-level sentiment signals."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID, uuid5

from kairos_core.contracts import SentimentSignal
from kairos_core.enums import ImpactDirection

from .models import NewsItem

_SIGNAL_NAMESPACE = UUID("d1fbb176-cdec-56ce-8a27-26d438c63929")


def _event_time(evidence: Sequence[NewsItem]) -> datetime:
    times = [
        item.published_at.replace(tzinfo=UTC)
        if item.published_at.tzinfo is None
        else item.published_at.astimezone(UTC)
        for item in evidence
    ]
    return max(times)


def _signal_id(
    *,
    source: str,
    topic: str,
    impact: str,
    sources: Sequence[str],
    event_time: datetime,
) -> str:
    canonical = json.dumps(
        {
            "source": source,
            "topic": topic.strip().casefold(),
            "impact": impact,
            "sources": list(sources),
            "event_time": event_time.isoformat(),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return str(uuid5(_SIGNAL_NAMESPACE, canonical))


def build_signal(
    *,
    source: str,
    topic: str,
    sentiment: float,
    impact: ImpactDirection,
    confidence: float,
    sources: Sequence[str],
    summary: str,
    evidence: Sequence[NewsItem],
) -> SentimentSignal:
    """Build a signal whose envelope is stable across replay of identical evidence."""
    event_time = _event_time(evidence)
    return SentimentSignal(
        message_id=_signal_id(
            source=source,
            topic=topic,
            impact=impact.value,
            sources=sources,
            event_time=event_time,
        ),
        produced_at=event_time,
        source=source,
        topic=topic,
        sentiment=sentiment,
        impact=impact,
        confidence=confidence,
        sources=list(sources),
        summary=summary,
    )

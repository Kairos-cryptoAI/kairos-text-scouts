"""Bound the event-time window accepted by the text signal contour."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta

from .models import NewsItem


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class EventFreshnessFilter:
    """Accept recent items while tolerating a small, explicit publisher clock skew."""

    def __init__(
        self,
        max_age_s: float,
        max_future_skew_s: float,
        *,
        allow_estimated_timestamps: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if max_age_s < 0 or max_future_skew_s < 0:
            raise ValueError("freshness bounds must be non-negative")
        self.max_age_s = max_age_s
        self.max_future_skew_s = max_future_skew_s
        self.allow_estimated_timestamps = allow_estimated_timestamps
        self._clock = clock

    def select(self, items: Iterable[NewsItem]) -> list[NewsItem]:
        now = _as_utc(self._clock())
        oldest = now - timedelta(seconds=self.max_age_s)
        newest = now + timedelta(seconds=self.max_future_skew_s)
        return [
            item
            for item in items
            if (self.allow_estimated_timestamps or not item.timestamp_is_estimated)
            and oldest <= _as_utc(item.published_at) <= newest
        ]

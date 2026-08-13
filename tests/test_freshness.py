from datetime import UTC, datetime, timedelta

from kairos_text.freshness import EventFreshnessFilter
from kairos_text.models import NewsItem

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=UTC)


def _item(offset_s: float, *, estimated: bool = False) -> NewsItem:
    return NewsItem(
        title=f"Bitcoin event {offset_s}",
        url=f"https://news.test/{offset_s}",
        published_at=NOW + timedelta(seconds=offset_s),
        timestamp_is_estimated=estimated,
    )


def test_freshness_bounds_are_inclusive_and_future_skew_is_bounded():
    selector = EventFreshnessFilter(1800, 120, clock=lambda: NOW)
    items = [_item(-1801), _item(-1800), _item(120), _item(121)]
    assert selector.select(items) == items[1:3]


def test_estimated_timestamps_abstain_by_default():
    estimated = _item(0, estimated=True)
    assert EventFreshnessFilter(1800, 120, clock=lambda: NOW).select([estimated]) == []
    assert EventFreshnessFilter(1800, 120, allow_estimated_timestamps=True, clock=lambda: NOW).select(
        [estimated]
    ) == [estimated]

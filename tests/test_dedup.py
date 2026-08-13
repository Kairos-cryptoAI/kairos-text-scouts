from datetime import UTC, datetime, timedelta

from kairos_text.dedup import EventDeduplicator, dedup_key, normalize_title
from kairos_text.models import NewsItem


def test_normalize_title_is_punctuation_and_case_insensitive():
    assert normalize_title("SEC approves ETF!") == normalize_title("sec approves   etf")


def test_collapses_same_canonical_url_within_a_batch():
    dedup = EventDeduplicator(window_s=100.0, clock=lambda: 0.0)
    items = [
        NewsItem(title="A", url="https://x.com/a?utm_source=feed"),
        NewsItem(title="B", url="https://x.com/a/"),
    ]
    assert len(dedup.filter_new(items)) == 1  # same url after canonicalization


def test_preserves_semantic_query_parameters_and_independent_events():
    left = NewsItem(title="BTC liquidations page 1", url="https://news.test/story?id=1&utm_source=x")
    right = NewsItem(title="BTC liquidations page 2", url="https://news.test/story?id=2&utm_source=x")
    assert dedup_key(left) != dedup_key(right)
    assert len(EventDeduplicator().filter_new([left, right])) == 2


def test_duplicate_merge_preserves_stable_provenance_and_best_record():
    now = datetime(2026, 8, 13, 9, tzinfo=UTC)
    short = NewsItem(
        title="SEC approves Bitcoin ETF",
        url="https://news.test/etf?utm_source=rss",
        source="feed",
        published_at=now,
        timestamp_is_estimated=False,
    )
    rich = NewsItem(
        title="SEC approves Bitcoin ETF",
        body="The regulator approved the spot fund after a vote.",
        url="https://news.test/etf?fbclid=tracking",
        source="wire",
        published_at=now + timedelta(seconds=1),
        timestamp_is_estimated=False,
    )
    dedup = EventDeduplicator()
    forward = dedup.collapse_batch([short, rich])
    reverse = dedup.collapse_batch([rich, short])
    assert forward == reverse
    assert forward[0].body == rich.body
    assert forward[0].provenance_refs == ("https://news.test/etf",)


def test_filter_unseen_does_not_consume_items_until_remembered():
    item = NewsItem(title="Bitcoin ETF", url="https://news.test/etf")
    dedup = EventDeduplicator()
    assert dedup.filter_unseen([item]) == [item]
    assert dedup.filter_unseen([item]) == [item]
    dedup.remember([item])
    assert dedup.filter_unseen([item]) == []


def test_title_dedup_when_no_url():
    dedup = EventDeduplicator(window_s=100.0, clock=lambda: 0.0)
    items = [NewsItem(title="SEC approves ETF!"), NewsItem(title="sec approves etf")]
    assert len(dedup.filter_new(items)) == 1


def test_remembers_across_polls_then_evicts_after_window():
    now = {"t": 0.0}
    dedup = EventDeduplicator(window_s=10.0, clock=lambda: now["t"])
    batch = [NewsItem(title="X", url="https://x.com/a")]
    assert len(dedup.filter_new(batch)) == 1  # first time -> fresh
    assert len(dedup.filter_new(batch)) == 0  # still within window -> suppressed
    now["t"] = 10.0
    assert len(dedup.filter_new(batch)) == 1  # window passed -> fresh again

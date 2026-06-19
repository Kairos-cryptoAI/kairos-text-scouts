from kairos_text.dedup import EventDeduplicator, normalize_title
from kairos_text.models import NewsItem


def test_normalize_title_is_punctuation_and_case_insensitive():
    assert normalize_title("SEC approves ETF!") == normalize_title("sec approves   etf")


def test_collapses_same_canonical_url_within_a_batch():
    dedup = EventDeduplicator(window_s=100.0, clock=lambda: 0.0)
    items = [NewsItem(title="A", url="https://x.com/a?utm=1"),
             NewsItem(title="B", url="https://x.com/a/")]
    assert len(dedup.filter_new(items)) == 1     # same url after canonicalization


def test_title_dedup_when_no_url():
    dedup = EventDeduplicator(window_s=100.0, clock=lambda: 0.0)
    items = [NewsItem(title="SEC approves ETF!"), NewsItem(title="sec approves etf")]
    assert len(dedup.filter_new(items)) == 1


def test_remembers_across_polls_then_evicts_after_window():
    now = {"t": 0.0}
    dedup = EventDeduplicator(window_s=10.0, clock=lambda: now["t"])
    batch = [NewsItem(title="X", url="https://x.com/a")]
    assert len(dedup.filter_new(batch)) == 1      # first time -> fresh
    assert len(dedup.filter_new(batch)) == 0      # still within window -> suppressed
    now["t"] = 20.0
    assert len(dedup.filter_new(batch)) == 1      # window passed -> fresh again

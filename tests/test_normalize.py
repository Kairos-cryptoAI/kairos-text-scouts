from datetime import datetime

from kairos_text.models import NewsItem
from kairos_text.normalize import EventNormalizer, clean


def test_clean_strips_tags_entities_and_whitespace():
    assert clean("  Fed &amp; <b>SEC</b>   meet\n\n") == "Fed & SEC meet"


def test_normalize_drops_empty_titles_bounds_length_and_fills_source():
    norm = EventNormalizer(max_title=10, max_body=5)
    items = [
        NewsItem(title="<i></i>   "),  # empty after clean -> dropped
        NewsItem(title="Bitcoin ETF approval", body="long body text", source="", source_kind="gdelt"),
    ]
    out = norm.normalize(items)
    assert len(out) == 1
    assert out[0].title == "Bitcoin ET"  # truncated to max_title
    assert out[0].body == "long "  # truncated to max_body
    assert out[0].source == "gdelt"  # empty source falls back to source_kind


def test_normalize_does_not_mutate_source_items_and_makes_time_utc_aware():
    original = NewsItem(title="  Bitcoin ETF  ", published_at=datetime(2026, 8, 13, 9))
    normalized = EventNormalizer().normalize([original])[0]
    assert original.title == "  Bitcoin ETF  "
    assert original.published_at.tzinfo is None
    assert normalized.title == "Bitcoin ETF"
    assert normalized.published_at.tzinfo is not None

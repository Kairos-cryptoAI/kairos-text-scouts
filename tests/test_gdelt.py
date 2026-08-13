from kairos_text.sources.gdelt import GDELTSource

SAMPLE = {
    "articles": [
        {
            "url": "https://coindesk.com/x",
            "title": "SEC approves spot Bitcoin ETF",
            "seendate": "20260619T141500Z",
            "domain": "coindesk.com",
            "language": "English",
        },
        {
            "url": "https://reuters.com/y",
            "title": "",
            "seendate": "20260619T141500Z",
            "domain": "reuters.com",
        },  # empty title -> skipped
        {
            "url": "https://cnbc.com/z",
            "title": "Fed holds rates",
            "seendate": "garbage",
            "domain": "cnbc.com",
        },  # bad date -> skipped instead of pretending it is fresh
    ]
}


def test_parse_maps_articles_and_skips_empty_titles():
    items = GDELTSource.parse(SAMPLE)
    assert len(items) == 1
    assert items[0].source_kind == "gdelt"
    assert items[0].source == "coindesk.com"
    assert items[0].url == "https://coindesk.com/x"
    assert items[0].published_at.year == 2026 and items[0].published_at.month == 6
    assert items[0].timestamp_is_estimated is False


def test_parse_handles_empty_or_missing_payload():
    assert GDELTSource.parse({}) == []
    assert GDELTSource.parse({"articles": None}) == []


def test_enabled_requires_a_query():
    assert GDELTSource(query="bitcoin").enabled is True
    assert GDELTSource(query="").enabled is False

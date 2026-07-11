"""RSS parser correctness and hostile-XML regression tests."""
from kairos_text.sources.rss import RSSSource


def test_parse_valid_rss_item():
    xml = """<rss><channel><item><title>BTC rally</title>
    <description><![CDATA[<b>Strong</b> demand]]></description>
    <link>https://example.com/btc</link></item></channel></rss>"""

    items = RSSSource._parse(xml, "https://example.com/feed")

    assert len(items) == 1
    assert items[0].title == "BTC rally"
    assert items[0].body == "Strong demand"


def test_parse_rejects_entity_expansion_payload():
    xml = """<?xml version="1.0"?>
    <!DOCTYPE rss [
      <!ENTITY a "1234567890">
      <!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">
    ]>
    <rss><channel><item><title>&b;</title></item></channel></rss>"""

    assert RSSSource._parse(xml, "hostile-feed") == []

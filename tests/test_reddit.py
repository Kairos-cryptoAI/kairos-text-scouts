from kairos_text.sources.reddit import RedditSource

LISTING = {"kind": "Listing", "data": {"children": [
    {"kind": "t3", "data": {
        "title": "ETF inflows hit a record", "selftext": "big day for spot ETFs",
        "subreddit": "CryptoCurrency", "score": 4200, "created_utc": 1718800000.0,
        "permalink": "/r/CryptoCurrency/comments/abc/etf_inflows/"}},
    {"kind": "t3", "data": {"title": "", "subreddit": "Bitcoin", "score": 5}},  # empty title -> skipped
]}}


def test_parse_listing_maps_posts_and_skips_empty_titles():
    out = RedditSource.parse_listing(LISTING)
    assert len(out) == 1
    assert out[0].source_kind == "reddit"
    assert out[0].source == "r/CryptoCurrency"
    assert out[0].engagement == 4200.0
    assert out[0].url == "https://www.reddit.com/r/CryptoCurrency/comments/abc/etf_inflows/"
    assert out[0].published_at.tzinfo is not None and out[0].published_at.year >= 2024


def test_parse_listing_handles_empty_payload():
    assert RedditSource.parse_listing({}) == []
    assert RedditSource.parse_listing({"data": {"children": []}}) == []


def test_enabled_requires_credentials_and_subreddits():
    assert RedditSource(client_id="", client_secret="", user_agent="ua", subreddits=["x"]).enabled is False
    assert RedditSource(client_id="i", client_secret="s", user_agent="ua", subreddits=["x"]).enabled is True
    assert RedditSource(client_id="i", client_secret="s", user_agent="ua", subreddits=[]).enabled is False

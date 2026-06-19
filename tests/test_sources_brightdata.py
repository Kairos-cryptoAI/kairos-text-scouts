from kairos_text.sources.brightdata_x import BrightDataXSource


def test_x_parse_maps_text_account_and_engagement():
    recs = [
        {"description": "BTC to the moon", "user_posted": "elonmusk",
         "url": "https://x.com/elonmusk/1", "likes": 1000, "reposts": 200,
         "date_posted": "2026-06-19T14:15:00Z"},
        {"text": "", "user_posted": "spam"},          # empty text -> skipped
    ]
    out = BrightDataXSource.parse(recs)
    assert len(out) == 1
    assert out[0].source_kind == "x"
    assert out[0].source == "elonmusk"
    assert out[0].engagement == 1200.0               # likes + reposts
    assert out[0].published_at.year == 2026


def test_x_enabled_only_with_token_dataset_and_accounts():
    assert BrightDataXSource(token="", dataset_id="", accounts=["a"]).enabled is False
    assert BrightDataXSource(token="t", dataset_id="d", accounts=["a"]).enabled is True
    assert BrightDataXSource(token="t", dataset_id="d", accounts=[]).enabled is False


def test_x_inputs_build_profile_urls_and_strip_at():
    src = BrightDataXSource(token="t", dataset_id="d", accounts=["@elonmusk", "cz_binance"], num_posts=5)
    inputs = src._inputs()
    assert inputs[0] == {"url": "https://x.com/elonmusk", "num_of_posts": 5}
    assert inputs[1]["url"] == "https://x.com/cz_binance"

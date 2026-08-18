import json
from datetime import UTC, datetime, timedelta

import pytest

from kairos_text.config import TextSettings
from kairos_text.feed_qualification import (
    FeedSpec,
    FeedStatus,
    _build_feed_specs,
    _read_secret,
    _write_report,
    qualify_feeds,
)
from kairos_text.models import NewsItem

NOW = datetime(2026, 8, 18, tzinfo=UTC)


def _item(*, age_s: float = 10, url: str = "https://example.com/news") -> NewsItem:
    return NewsItem(
        title="Bitcoin market update",
        url=url,
        source="example.com",
        source_kind="test",
        published_at=NOW - timedelta(seconds=age_s),
        timestamp_is_estimated=False,
    )


async def _no_sleep(_seconds):
    return None


@pytest.mark.asyncio
async def test_fresh_available_feed_is_still_blocked_until_quota_is_observed():
    async def fetch():
        return [_item()]

    report = await qualify_feeds(
        feeds=[FeedSpec("free", "free", fetch)],
        samples_per_feed=3,
        interval_s=0,
        clock=lambda: NOW,
        sleep=_no_sleep,
    )

    assert report.status is FeedStatus.BLOCKED
    summary = report.feeds[0]
    assert summary.availability == 1
    assert summary.total_fresh_items == 3
    assert summary.reasons == ("quota_unobserved",)
    assert report.live_orders_allowed is False


@pytest.mark.asyncio
async def test_missing_credentials_make_no_request_and_remain_blocked():
    report = await qualify_feeds(
        feeds=[FeedSpec("optional", "free", None, "credentials missing")],
        samples_per_feed=2,
        interval_s=0,
        clock=lambda: NOW,
        sleep=_no_sleep,
    )

    assert report.status is FeedStatus.BLOCKED
    assert all(item.status is FeedStatus.BLOCKED for item in report.samples)
    assert report.feeds[0].total_items == 0


@pytest.mark.asyncio
async def test_empty_and_stale_content_block_while_invalid_provenance_fails():
    async def empty():
        return []

    async def stale():
        return [_item(age_s=2_000)]

    async def invalid():
        return [_item(url="")]

    report = await qualify_feeds(
        feeds=[
            FeedSpec("empty", "free", empty),
            FeedSpec("stale", "free", stale),
            FeedSpec("invalid", "free", invalid),
        ],
        samples_per_feed=1,
        interval_s=0,
        clock=lambda: NOW,
        sleep=_no_sleep,
    )

    assert report.status is FeedStatus.FAIL
    by_feed = {item.feed: item.status for item in report.samples}
    assert by_feed == {
        "empty": FeedStatus.BLOCKED,
        "stale": FeedStatus.BLOCKED,
        "invalid": FeedStatus.FAIL,
    }


def test_real_feed_specs_never_enable_metered_probe_implicitly():
    specs = _build_feed_specs(
        TextSettings(),
        reddit_client_id="",
        reddit_client_secret="",
        brightdata_token="token",
        brightdata_dataset_id="dataset",
        allow_metered_brightdata_probe=False,
    )

    brightdata = next(item for item in specs if item.name == "brightdata_x")
    reddit = next(item for item in specs if item.name == "reddit")
    assert brightdata.fetcher is None
    assert "--allow-metered" in (brightdata.blocked_reason or "")
    assert reddit.fetcher is None


@pytest.mark.asyncio
async def test_report_writer_refuses_overwrite(tmp_path):
    async def fetch():
        return [_item()]

    report = await qualify_feeds(
        feeds=[FeedSpec("free", "free", fetch)],
        samples_per_feed=1,
        interval_s=0,
        clock=lambda: NOW,
        sleep=_no_sleep,
    )
    destination = tmp_path / "feed-report.json"

    _write_report(destination, report, overwrite=False)

    assert json.loads(destination.read_text(encoding="utf-8"))["live_orders_allowed"] is False
    with pytest.raises(FileExistsError):
        _write_report(destination, report, overwrite=False)


def test_secret_file_must_not_be_empty(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        _read_secret(secret, "provider")

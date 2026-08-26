import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kairos_text.config import TextSettings
from kairos_text.feed_qualification import (
    FeedSpec,
    FeedStatus,
    _build_feed_specs,
    _QualificationState,
    _read_secret,
    _usd_to_microusd,
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
async def test_metered_feed_passes_with_quota_and_exact_usage_evidence():
    async def fetch():
        return [_item()]

    report = await qualify_feeds(
        feeds=[
            FeedSpec(
                "x_api",
                "metered_capped",
                fetch,
                quota_observer=lambda: True,
                usage_observer=lambda: (10, 50_000),
            )
        ],
        samples_per_feed=1,
        interval_s=0,
        clock=lambda: NOW,
        sleep=_no_sleep,
    )

    summary = report.feeds[0]
    assert summary.status is FeedStatus.PASS
    assert summary.quota_observed
    assert summary.metered_units == 10
    assert summary.estimated_cost_usd == "0.050000"
    assert report.to_dict()["schema_version"] == 2


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
        x_bearer_token="token",
        allow_metered_x_probe=False,
        maximum_x_cost_microusd=200_000,
    )

    x_api = next(item for item in specs if item.name == "x_api")
    reddit = next(item for item in specs if item.name == "reddit")
    assert x_api.fetcher is None
    assert x_api.cost_mode == "metered_capped"
    assert "--allow-metered" in (x_api.blocked_reason or "")
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


@pytest.mark.parametrize(
    ("usd", "microusd"),
    [(Decimal("0.20"), 200_000), (Decimal("2"), 2_000_000)],
)
def test_x_probe_cost_cap_uses_exact_decimal_micro_usd(usd, microusd):
    assert _usd_to_microusd(usd) == microusd


@pytest.mark.parametrize("usd", [Decimal("0"), Decimal("2.000001"), Decimal("0.0000001")])
def test_x_probe_cost_cap_rejects_zero_excessive_and_sub_micro_values(usd):
    with pytest.raises(ValueError):
        _usd_to_microusd(usd)


@pytest.mark.asyncio
async def test_ambiguous_qualification_reservation_is_reported_as_potential_spend():
    state = _QualificationState(40_000)
    await state.reserve_usage(
        service="text-scouts:qualification",
        source="x",
        reservation_id="ambiguous-request",
        reserved_units=5,
        unit_cost_microusd=5_000,
        monthly_budget_microusd=40_000,
    )

    assert state.usage() == (5, 25_000)

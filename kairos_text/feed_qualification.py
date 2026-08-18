"""Read-only and explicitly metered source availability qualification."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import tempfile
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from .config import TextSettings
from .models import NewsItem
from .sources import BrightDataXSource, GDELTSource, RedditSource, RSSSource


class FeedStatus(StrEnum):
    PASS = "PASS"  # nosec B105
    BLOCKED = "BLOCKED"
    FAIL = "FAIL"


@dataclass(frozen=True)
class FeedSpec:
    name: str
    cost_mode: Literal["free", "metered_unverified"]
    fetcher: Callable[[], Awaitable[list[NewsItem]]] | None
    blocked_reason: str | None = None


@dataclass(frozen=True)
class FeedSample:
    feed: str
    sample: int
    status: FeedStatus
    latency_s: float | None
    items: int
    fresh_items: int
    latest_age_s: float | None
    detail: str


@dataclass(frozen=True)
class FeedSummary:
    feed: str
    cost_mode: str
    samples_requested: int
    successful_samples: int
    availability: float
    total_items: int
    total_fresh_items: int
    p95_latency_s: float | None
    quota_observed: bool
    status: FeedStatus
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class FeedQualificationReport:
    schema_version: int
    generated_at: str
    samples_per_feed: int
    interval_s: float
    maximum_item_age_s: float
    maximum_latency_s: float
    samples: tuple[FeedSample, ...]
    feeds: tuple[FeedSummary, ...]
    live_orders_allowed: bool = False

    @property
    def status(self) -> FeedStatus:
        statuses = {item.status for item in self.feeds}
        if FeedStatus.FAIL in statuses:
            return FeedStatus.FAIL
        if FeedStatus.BLOCKED in statuses:
            return FeedStatus.BLOCKED
        return FeedStatus.PASS

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "samples_per_feed": self.samples_per_feed,
            "interval_s": self.interval_s,
            "maximum_item_age_s": self.maximum_item_age_s,
            "maximum_latency_s": self.maximum_latency_s,
            "status": self.status.value,
            "live_orders_allowed": False,
            "samples": [asdict(item) for item in self.samples],
            "feeds": [asdict(item) for item in self.feeds],
        }


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _inspect_items(
    items: list[NewsItem], *, now: datetime, maximum_item_age_s: float
) -> tuple[int, float | None]:
    if not items:
        return 0, None
    fresh = 0
    latest_age: float | None = None
    for item in items:
        if not item.title.strip() or not item.source.strip() or not item.url.strip():
            raise ValueError("source returned an item without title/source/url provenance")
        if item.published_at is None or item.published_at.tzinfo is None:
            raise ValueError("source returned an item without an aware publication timestamp")
        age = (now - item.published_at.astimezone(UTC)).total_seconds()
        if age < -5:
            raise ValueError("source returned future-dated evidence")
        latest_age = max(0.0, age) if latest_age is None else min(latest_age, max(0.0, age))
        if age <= maximum_item_age_s:
            fresh += 1
    return fresh, latest_age


async def qualify_feeds(
    *,
    feeds: Sequence[FeedSpec],
    samples_per_feed: int = 3,
    interval_s: float = 5.0,
    maximum_item_age_s: float = 1_800.0,
    maximum_latency_s: float = 30.0,
    clock: Callable[[], datetime] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> FeedQualificationReport:
    if samples_per_feed <= 0:
        raise ValueError("samples_per_feed must be positive")
    if interval_s < 0 or not math.isfinite(interval_s):
        raise ValueError("interval_s must be finite and non-negative")
    if maximum_item_age_s <= 0 or maximum_latency_s <= 0:
        raise ValueError("qualification thresholds must be positive")
    if len({item.name for item in feeds}) != len(feeds):
        raise ValueError("feed names must be unique")
    now_fn = clock or (lambda: datetime.now(UTC))
    observations: list[FeedSample] = []
    for sample_index in range(1, samples_per_feed + 1):
        for feed in feeds:
            if feed.fetcher is None:
                observations.append(
                    FeedSample(
                        feed=feed.name,
                        sample=sample_index,
                        status=FeedStatus.BLOCKED,
                        latency_s=None,
                        items=0,
                        fresh_items=0,
                        latest_age_s=None,
                        detail=feed.blocked_reason or "feed is not configured",
                    )
                )
                continue
            started = time.perf_counter()
            try:
                items = await feed.fetcher()
                latency = time.perf_counter() - started
                fresh, latest_age = _inspect_items(
                    items,
                    now=now_fn().astimezone(UTC),
                    maximum_item_age_s=maximum_item_age_s,
                )
                if not items:
                    status = FeedStatus.BLOCKED
                    detail = (
                        "source returned no items; transport and content inactivity are not distinguishable"
                    )
                elif fresh == 0:
                    status = FeedStatus.BLOCKED
                    detail = "source returned no item inside the freshness window"
                elif latency > maximum_latency_s:
                    status = FeedStatus.FAIL
                    detail = "source latency exceeded the registered limit"
                else:
                    status = FeedStatus.PASS
                    detail = "attributable fresh items validated"
                observations.append(
                    FeedSample(
                        feed=feed.name,
                        sample=sample_index,
                        status=status,
                        latency_s=latency,
                        items=len(items),
                        fresh_items=fresh,
                        latest_age_s=latest_age,
                        detail=detail,
                    )
                )
            except Exception as exc:
                observations.append(
                    FeedSample(
                        feed=feed.name,
                        sample=sample_index,
                        status=FeedStatus.FAIL,
                        latency_s=time.perf_counter() - started,
                        items=0,
                        fresh_items=0,
                        latest_age_s=None,
                        detail=f"{type(exc).__name__}: {str(exc)[:300]}",
                    )
                )
        if sample_index < samples_per_feed:
            await sleep(interval_s)
    summaries: list[FeedSummary] = []
    for feed in feeds:
        samples = [item for item in observations if item.feed == feed.name]
        successful = [item for item in samples if item.status is FeedStatus.PASS]
        reasons: list[str] = []
        if feed.fetcher is None:
            reasons.append("credentials_or_explicit_metered_authorization_missing")
        if len(successful) != samples_per_feed:
            reasons.append("availability_below_threshold")
        if feed.cost_mode == "metered_unverified" and feed.fetcher is not None:
            reasons.append("metered_cost_unverified")
        # Current providers do not expose quota metadata through EventSource.
        # Preserve this as an explicit blocker rather than inventing a quota.
        reasons.append("quota_unobserved")
        status = (
            FeedStatus.FAIL
            if any(item.status is FeedStatus.FAIL for item in samples)
            else FeedStatus.PASS
            if not reasons
            else FeedStatus.BLOCKED
        )
        summaries.append(
            FeedSummary(
                feed=feed.name,
                cost_mode=feed.cost_mode,
                samples_requested=samples_per_feed,
                successful_samples=len(successful),
                availability=len(successful) / samples_per_feed,
                total_items=sum(item.items for item in samples),
                total_fresh_items=sum(item.fresh_items for item in samples),
                p95_latency_s=_percentile(
                    [item.latency_s for item in samples if item.latency_s is not None], 0.95
                ),
                quota_observed=False,
                status=status,
                reasons=tuple(reasons),
            )
        )
    return FeedQualificationReport(
        schema_version=1,
        generated_at=now_fn().astimezone(UTC).isoformat(),
        samples_per_feed=samples_per_feed,
        interval_s=interval_s,
        maximum_item_age_s=maximum_item_age_s,
        maximum_latency_s=maximum_latency_s,
        samples=tuple(observations),
        feeds=tuple(summaries),
    )


def _read_secret(path: Path | None, name: str) -> str:
    if path is None:
        return ""
    value = path.resolve().read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"{name} secret file is empty")
    return value


def _build_feed_specs(
    settings: TextSettings,
    *,
    reddit_client_id: str,
    reddit_client_secret: str,
    brightdata_token: str,
    brightdata_dataset_id: str,
    allow_metered_brightdata_probe: bool,
) -> list[FeedSpec]:
    feeds: list[FeedSpec] = [
        FeedSpec(
            "gdelt",
            "free",
            GDELTSource(
                query=settings.gdelt_query,
                timespan=settings.gdelt_timespan,
                max_records=settings.gdelt_max_records,
            ).fetch,
        )
    ]
    feeds.extend(
        FeedSpec(f"rss:{index + 1}", "free", RSSSource([url]).fetch)
        for index, url in enumerate(settings.rss_feeds)
    )
    reddit = RedditSource(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=settings.reddit_user_agent,
        subreddits=settings.subreddits,
        listing=settings.reddit_listing,
        limit=settings.reddit_limit,
    )
    feeds.append(
        FeedSpec(
            "reddit",
            "free",
            reddit.fetch if reddit.enabled else None,
            "Reddit client ID/secret files were not supplied",
        )
    )
    brightdata = BrightDataXSource(
        token=brightdata_token,
        dataset_id=brightdata_dataset_id,
        accounts=settings.x_accounts,
        num_posts=settings.x_num_posts,
        poll_timeout_s=settings.brightdata_poll_timeout_s,
    )
    brightdata_allowed = brightdata.enabled and allow_metered_brightdata_probe
    feeds.append(
        FeedSpec(
            "brightdata_x",
            "metered_unverified",
            brightdata.fetch if brightdata_allowed else None,
            (
                "Bright Data token/dataset is missing"
                if not brightdata.enabled
                else "metered probe requires --allow-metered-brightdata-probe"
            ),
        )
    )
    return feeds


def _write_report(path: Path, report: FeedQualificationReport, *, overwrite: bool) -> None:
    resolved = path.resolve()
    if resolved.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite feed qualification report: {resolved}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report.to_dict(), sort_keys=True, indent=2, allow_nan=False) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{resolved.name}.", suffix=".tmp", dir=resolved.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
    finally:
        if temporary.exists():
            temporary.unlink()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Qualify Text Scouts source feeds")
    parser.add_argument("--reddit-client-id-file", type=Path)
    parser.add_argument("--reddit-client-secret-file", type=Path)
    parser.add_argument("--brightdata-token-file", type=Path)
    parser.add_argument("--brightdata-dataset-id", default="")
    parser.add_argument("--allow-metered-brightdata-probe", action="store_true")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = TextSettings()
    feeds = _build_feed_specs(
        settings,
        reddit_client_id=_read_secret(args.reddit_client_id_file, "Reddit client ID"),
        reddit_client_secret=_read_secret(args.reddit_client_secret_file, "Reddit client secret"),
        brightdata_token=_read_secret(args.brightdata_token_file, "Bright Data token"),
        brightdata_dataset_id=args.brightdata_dataset_id,
        allow_metered_brightdata_probe=args.allow_metered_brightdata_probe,
    )
    report = asyncio.run(
        qualify_feeds(
            feeds=feeds,
            samples_per_feed=args.samples,
            interval_s=args.interval_s,
            maximum_item_age_s=settings.max_event_age_s,
        )
    )
    _write_report(args.output, report, overwrite=args.overwrite)
    print(f"Feed qualification: {report.status.value}; live_orders_allowed=false")
    return 0 if report.status is FeedStatus.PASS else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

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
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Literal

from kairos_persistence import SourceBudgetExceeded, SourceCursor

from .config import TextSettings
from .models import NewsItem
from .sources import GDELTSource, RedditSource, RSSSource, XApiSource


class FeedStatus(StrEnum):
    PASS = "PASS"  # nosec B105
    BLOCKED = "BLOCKED"
    FAIL = "FAIL"


@dataclass(frozen=True)
class FeedSpec:
    name: str
    cost_mode: Literal["free", "metered_capped"]
    fetcher: Callable[[], Awaitable[list[NewsItem]]] | None
    blocked_reason: str | None = None
    quota_observer: Callable[[], bool] | None = None
    usage_observer: Callable[[], tuple[int, int]] | None = None


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
    metered_units: int
    estimated_cost_usd: str
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
        quota_observed = feed.quota_observer() if feed.quota_observer is not None else False
        if not quota_observed:
            reasons.append("quota_unobserved")
        metered_units, cost_microusd = feed.usage_observer() if feed.usage_observer is not None else (0, 0)
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
                quota_observed=quota_observed,
                metered_units=metered_units,
                estimated_cost_usd=_microusd(cost_microusd),
                status=status,
                reasons=tuple(reasons),
            )
        )
    return FeedQualificationReport(
        schema_version=2,
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


class _QualificationState:
    """Process-local hard cap for one explicitly authorized qualification run."""

    def __init__(self, maximum_cost_microusd: int) -> None:
        if maximum_cost_microusd <= 0:
            raise ValueError("maximum X qualification cost must be positive")
        self.maximum_cost_microusd = maximum_cost_microusd
        self.cursors: dict[tuple[str, str, str], SourceCursor] = {}
        self.reservations: dict[tuple[str, str, str], tuple[int, int, str, int | None]] = {}
        self.committed_units = 0
        self.committed_cost_microusd = 0

    async def get_cursor(self, service: str, source: str, cursor_key: str) -> SourceCursor | None:
        return self.cursors.get((service, source, cursor_key))

    async def advance_cursor(self, service: str, source: str, cursor_key: str, cursor_value: str) -> bool:
        key = (service, source, cursor_key)
        current = self.cursors.get(key)
        if current is not None and int(cursor_value) < int(current.cursor_value):
            raise ValueError("qualification cursor regression")
        changed = current is None or current.cursor_value != cursor_value
        self.cursors[key] = SourceCursor(
            service=service,
            source=source,
            cursor_key=cursor_key,
            cursor_value=cursor_value,
            updated_at=datetime.now(UTC),
        )
        return changed

    async def reserve_usage(
        self,
        *,
        service: str,
        source: str,
        reservation_id: str,
        reserved_units: int,
        unit_cost_microusd: int,
        monthly_budget_microusd: int,
        requested_at: datetime | None = None,
    ) -> object:
        del requested_at
        if monthly_budget_microusd != self.maximum_cost_microusd:
            raise ValueError("qualification source budget does not match the registered hard cap")
        key = (service, source, reservation_id)
        if key in self.reservations:
            raise ValueError("qualification reservation ID was reused")
        outstanding = sum(
            units * unit_cost
            for units, unit_cost, status, _actual in self.reservations.values()
            if status == "RESERVED"
        )
        requested = reserved_units * unit_cost_microusd
        if self.committed_cost_microusd + outstanding + requested > self.maximum_cost_microusd:
            raise SourceBudgetExceeded("X qualification hard cost cap would be exceeded")
        self.reservations[key] = (reserved_units, unit_cost_microusd, "RESERVED", None)
        return object()

    async def commit_usage(self, service: str, source: str, reservation_id: str, actual_units: int) -> object:
        key = (service, source, reservation_id)
        reserved, unit_cost, status, previous_actual = self.reservations[key]
        if actual_units > reserved:
            raise ValueError("qualification actual units exceed reservation")
        if status == "COMMITTED":
            if previous_actual != actual_units:
                raise ValueError("qualification committed units changed")
            return object()
        if status != "RESERVED":
            raise ValueError("qualification reservation was released")
        self.reservations[key] = (reserved, unit_cost, "COMMITTED", actual_units)
        self.committed_units += actual_units
        self.committed_cost_microusd += actual_units * unit_cost
        return object()

    async def release_usage(self, service: str, source: str, reservation_id: str) -> object:
        key = (service, source, reservation_id)
        reserved, unit_cost, status, actual = self.reservations[key]
        if status == "COMMITTED":
            raise ValueError("qualification committed usage cannot be released")
        self.reservations[key] = (reserved, unit_cost, "RELEASED", actual)
        return object()

    def usage(self) -> tuple[int, int]:
        return self.committed_units, self.committed_cost_microusd


def _microusd(value: int) -> str:
    return f"{Decimal(value) / Decimal(1_000_000):.6f}"


def _usd_to_microusd(value: Decimal) -> int:
    if not value.is_finite() or value <= 0 or value > Decimal("2"):
        raise ValueError("maximum X qualification cost must be in (0, 2] USD")
    micros = value * Decimal(1_000_000)
    if micros != micros.to_integral_value():
        raise ValueError("maximum X qualification cost supports at most 6 decimal places")
    return int(micros)


def _decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("expected a decimal USD amount") from exc


def _build_feed_specs(
    settings: TextSettings,
    *,
    reddit_client_id: str,
    reddit_client_secret: str,
    x_bearer_token: str,
    allow_metered_x_probe: bool,
    maximum_x_cost_microusd: int,
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
    qualification_state = _QualificationState(maximum_x_cost_microusd)
    x_source = XApiSource(
        bearer_token=x_bearer_token,
        accounts=settings.x_accounts,
        service_name=f"{settings.service_name}:qualification",
        max_results=settings.x_max_results,
        max_pages=settings.x_max_pages,
        timeout_s=settings.x_timeout_s,
        monthly_budget_microusd=maximum_x_cost_microusd,
        post_read_unit_cost_microusd=settings.x_post_read_unit_cost_microusd,
        user_read_unit_cost_microusd=settings.x_user_read_unit_cost_microusd,
        state=qualification_state,
    )

    async def fetch_x() -> list[NewsItem]:
        items = await x_source.fetch()
        await x_source.commit_fetch()
        return items

    x_allowed = x_source.enabled and allow_metered_x_probe
    feeds.append(
        FeedSpec(
            "x_api",
            "metered_capped",
            fetch_x if x_allowed else None,
            (
                "X Bearer Token file was not supplied"
                if not x_source.enabled
                else "metered probe requires --allow-metered-x-probe"
            ),
            quota_observer=lambda: x_source.rate_limit_observed,
            usage_observer=qualification_state.usage,
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
    parser.add_argument("--x-bearer-token-file", type=Path)
    parser.add_argument("--x-account", action="append", dest="x_accounts")
    parser.add_argument("--allow-metered-x-probe", action="store_true")
    parser.add_argument("--maximum-x-cost-usd", type=_decimal, default=Decimal("0.20"))
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = TextSettings()
    if args.x_accounts:
        settings = settings.model_copy(update={"x_accounts": args.x_accounts})
    maximum_x_cost_microusd = _usd_to_microusd(args.maximum_x_cost_usd)
    feeds = _build_feed_specs(
        settings,
        reddit_client_id=_read_secret(args.reddit_client_id_file, "Reddit client ID"),
        reddit_client_secret=_read_secret(args.reddit_client_secret_file, "Reddit client secret"),
        x_bearer_token=_read_secret(args.x_bearer_token_file, "X Bearer Token"),
        allow_metered_x_probe=args.allow_metered_x_probe,
        maximum_x_cost_microusd=maximum_x_cost_microusd,
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

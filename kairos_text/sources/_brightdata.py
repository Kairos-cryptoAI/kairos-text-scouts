"""Shared Bright Data Web Scraper API client (live, on-demand scraping).

This is Bright Data's *scraper*, NOT the static Dataset Marketplace: every call
collects fresh data at request time. We default to the **synchronous real-time**
endpoint (``POST /datasets/v3/scrape``, up to 20 URLs, results returned in the same
response) and fall back to the **asynchronous** trigger/poll/snapshot flow only if
Bright Data converts a long-running job to a snapshot (HTTP 202). Bright Data runs
the collection on its own managed infrastructure, so we never operate proxies,
rotate user-agents or fight captchas/403s ourselves.

All network here is ``pragma: no cover``; the per-record mappers in the X / Reddit
sources are pure and unit-tested.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import aiohttp

API_BASE = "https://api.brightdata.com/datasets/v3"
_POLL_TIMEOUT = aiohttp.ClientTimeout(total=30)
_SNAPSHOT_TIMEOUT = aiohttp.ClientTimeout(total=60)
_SCRAPE_TIMEOUT = aiohttp.ClientTimeout(total=90)


def first(record: dict[str, Any], keys: Sequence[str], default: str = "") -> Any:
    """First non-empty value among ``keys`` (record schemas vary by dataset)."""
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return default


def num(record: dict[str, Any], keys: Sequence[str]) -> float:
    """Sum of the numeric values found under ``keys`` (engagement metrics)."""
    total = 0.0
    for key in keys:
        value = record.get(key)
        try:
            if value is not None:
                total += float(value)
        except (TypeError, ValueError):
            continue
    return total


def to_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError):  # pragma: no cover - defensive
            return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", []) or []
    return []


async def _await_snapshot(  # pragma: no cover - network
    session: aiohttp.ClientSession,
    snapshot_id: str,
    *,
    base_url: str,
    poll_timeout_s: float,
    poll_interval_s: float,
) -> list[dict[str, Any]]:
    """Async fallback: poll a snapshot to completion, then download it."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + poll_timeout_s
    while loop.time() < deadline:
        async with session.get(f"{base_url}/progress/{snapshot_id}", timeout=_POLL_TIMEOUT) as resp:
            status = (await resp.json()).get("status") if resp.status == 200 else None
        if status == "ready":
            break
        if status in ("failed", "error"):
            return []
        await asyncio.sleep(poll_interval_s)
    else:
        return []
    async with session.get(
        f"{base_url}/snapshot/{snapshot_id}",
        params={"format": "json"},
        timeout=_SNAPSHOT_TIMEOUT,
    ) as resp:
        if resp.status != 200:
            return []
        return _rows(await resp.json(content_type=None))


async def collect(
    *,
    token: str,
    dataset_id: str,
    inputs: list[dict[str, Any]],
    poll_timeout_s: float = 90.0,
    base_url: str = API_BASE,
    poll_interval_s: float = 3.0,
) -> list[dict[str, Any]]:  # pragma: no cover - network
    """Scrape ``inputs`` live and return the records (or ``[]`` on any failure).

    Tries the synchronous real-time endpoint first (best for our <=20 handles /
    subreddits); if Bright Data converts the job to async (HTTP 202) we poll the
    returned snapshot instead.
    """
    if not (token and dataset_id and inputs):
        return []
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(
                f"{base_url}/scrape",
                params={"dataset_id": dataset_id, "format": "json", "include_errors": "true"},
                json={"input": list(inputs)},
                timeout=_SCRAPE_TIMEOUT,
            ) as resp:
                if resp.status == 200:
                    return _rows(await resp.json(content_type=None))  # fresh data, this request
                if resp.status != 202:
                    return []
                try:
                    snapshot_id: str | None = (await resp.json()).get("snapshot_id")
                except Exception:
                    snapshot_id = None
            if not snapshot_id:
                return []
            return await _await_snapshot(
                session,
                snapshot_id,
                base_url=base_url,
                poll_timeout_s=poll_timeout_s,
                poll_interval_s=poll_interval_s,
            )
    except Exception:
        return []

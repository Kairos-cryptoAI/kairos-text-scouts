"""Shared Bright Data Dataset API v3 client helpers.

Bright Data runs the collection on its own managed infrastructure, so we never
operate proxies, rotate user-agents or fight captchas/403s ourselves. The flow is:
``POST /trigger`` -> poll ``/progress/{snapshot}`` until ``ready`` -> ``GET
/snapshot/{snapshot}``. All network here is ``pragma: no cover``; the per-record
mappers in the X / Reddit sources are pure and unit-tested.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

API_BASE = "https://api.brightdata.com/datasets/v3"


def first(record: Dict[str, Any], keys: Sequence[str], default: str = "") -> Any:
    """First non-empty value among ``keys`` (record schemas vary by dataset)."""
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return default


def num(record: Dict[str, Any], keys: Sequence[str]) -> float:
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


def to_dt(value: Any) -> datetime:
    if value in (None, ""):
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):  # pragma: no cover - defensive
            return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


async def collect(*, token: str, dataset_id: str, inputs: List[Dict[str, Any]],
                  poll_timeout_s: float = 90.0, base_url: str = API_BASE,
                  poll_interval_s: float = 3.0) -> List[Dict[str, Any]]:  # pragma: no cover - network
    """Trigger a dataset collection and return its records (or ``[]`` on any failure)."""
    if aiohttp is None or not (token and dataset_id and inputs):
        return []
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    loop = asyncio.get_event_loop()
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(f"{base_url}/trigger",
                                    params={"dataset_id": dataset_id, "include_errors": "true"},
                                    json=inputs, timeout=30) as resp:
                if resp.status not in (200, 201):
                    return []
                snapshot_id: Optional[str] = (await resp.json()).get("snapshot_id")
            if not snapshot_id:
                return []
            deadline = loop.time() + poll_timeout_s
            while loop.time() < deadline:
                async with session.get(f"{base_url}/progress/{snapshot_id}", timeout=30) as resp:
                    status = (await resp.json()).get("status") if resp.status == 200 else None
                if status == "ready":
                    break
                if status in ("failed", "error"):
                    return []
                await asyncio.sleep(poll_interval_s)
            else:
                return []  # timed out waiting for the snapshot
            async with session.get(f"{base_url}/snapshot/{snapshot_id}",
                                   params={"format": "json"}, timeout=60) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
    except Exception:
        return []
    if isinstance(data, list):
        return data
    return data.get("data", []) if isinstance(data, dict) else []

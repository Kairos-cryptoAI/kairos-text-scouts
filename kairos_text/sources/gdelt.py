"""GDELT DOC 2.0 source — free, official global news aggregator.

Replaces per-publisher scraping: GDELT already indexes Reuters, Bloomberg, CNBC,
Yahoo Finance, Coindesk, Cointelegraph and thousands of outlets. We query the DOC
2.0 ``ArtList`` endpoint for recent English articles matching a crypto/macro query
and map every article to a :class:`NewsItem`. No API key, no proxy, negligible
block risk.

The endpoint is occasionally rate-limited and answers with an empty / non-JSON
body; :meth:`fetch` treats any such response as "no items this tick" rather than
raising, so a flaky GDELT never takes the layer down.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from ..models import NewsItem

try:
    import aiohttp
except Exception:  # pragma: no cover - aiohttp always present in prod
    aiohttp = None  # type: ignore

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
_UA = "kairos-text-scouts/0.1 (+https://github.com/Kairos-cryptoAI)"


def _parse_seendate(value: str) -> datetime:
    """GDELT timestamps look like ``20260619T141500Z``."""
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


class GDELTSource:
    name = "gdelt"

    def __init__(self, *, query: str, timespan: str = "15min", max_records: int = 75,
                 base_url: str = GDELT_DOC_URL, enabled: bool = True) -> None:
        self.query = query
        self.timespan = timespan
        self.max_records = max_records
        self.base_url = base_url
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self.query)

    @staticmethod
    def parse(payload: Dict[str, Any]) -> List[NewsItem]:
        """Map a GDELT DOC ArtList payload to NewsItems (pure, unit-tested)."""
        items: List[NewsItem] = []
        for art in (payload or {}).get("articles", []) or []:
            title = (art.get("title") or "").strip()
            if not title:
                continue
            items.append(NewsItem(
                title=title,
                url=(art.get("url") or "").strip(),
                source=(art.get("domain") or "gdelt").strip(),
                source_kind="gdelt",
                published_at=_parse_seendate(art.get("seendate", "")),
            ))
        return items

    async def fetch(self) -> List[NewsItem]:  # pragma: no cover - network
        if aiohttp is None:
            return []
        params = {
            "query": self.query, "mode": "ArtList", "maxrecords": str(self.max_records),
            "format": "json", "timespan": self.timespan, "sort": "DateDesc",
        }
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": _UA}) as session:
                async with session.get(self.base_url, params=params, timeout=20) as resp:
                    if resp.status != 200:
                        return []
                    payload = await resp.json(content_type=None)  # GDELT mislabels content-type
        except Exception:
            return []  # rate-limited / empty body / network blip -> skip this tick
        return self.parse(payload)

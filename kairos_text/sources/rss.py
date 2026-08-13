"""Minimal async RSS reader — the resilient fallback source.

GDELT is the primary news firehose; RSS stays as a cheap, dependency-light backstop
for a couple of crypto-native outlets (Coindesk, Cointelegraph). Reuters/Bloomberg
no longer publish public RSS, so we rely on GDELT for those. Parses only the handful
of fields we need; swap in ``feedparser`` behind the same ``fetch`` for production.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import aiohttp
from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException
from kairos_core.logging import get_logger

from ..models import NewsItem

_TAG = re.compile(r"<[^>]+>")
_UA = "kairos-text-scouts/0.1 (+https://github.com/Kairos-cryptoAI)"
_TIMEOUT = aiohttp.ClientTimeout(total=10)
log = get_logger("text-scouts.rss")


def _parse_date(value: str) -> datetime | None:
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class RSSSource:
    name = "rss"

    def __init__(self, feeds: list[str], *, enabled: bool = True) -> None:
        self.feeds = feeds
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self.feeds)

    @staticmethod
    def _parse(xml: str, source: str) -> list[NewsItem]:
        items: list[NewsItem] = []
        try:
            root = ET.fromstring(xml)
        except (ET.ParseError, DefusedXmlException):
            return items
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            desc = _TAG.sub("", item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            published_at = _parse_date(item.findtext("pubDate") or "")
            if title and published_at is not None:
                items.append(
                    NewsItem(
                        title=title,
                        body=desc,
                        url=link,
                        source=source,
                        source_kind="rss",
                        published_at=published_at,
                        timestamp_is_estimated=False,
                    )
                )
        return items

    async def fetch(self) -> list[NewsItem]:  # pragma: no cover - network
        out: list[NewsItem] = []
        async with aiohttp.ClientSession(headers={"User-Agent": _UA}) as session:
            for feed in self.feeds:
                try:
                    async with session.get(feed, timeout=_TIMEOUT) as resp:
                        if resp.status == 200:
                            out.extend(self._parse(await resp.text(), feed))
                except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                    log.warning("rss_fetch_failed", feed=feed, error=str(exc))
        return out

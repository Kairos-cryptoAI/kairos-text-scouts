"""Minimal async RSS reader — the resilient fallback source.

GDELT is the primary news firehose; RSS stays as a cheap, dependency-light backstop
for a couple of crypto-native outlets (Coindesk, Cointelegraph). Reuters/Bloomberg
no longer publish public RSS, so we rely on GDELT for those. Parses only the handful
of fields we need; swap in ``feedparser`` behind the same ``fetch`` for production.
"""
from __future__ import annotations

import re
from typing import List

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

from ..models import NewsItem

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

_TAG = re.compile(r"<[^>]+>")
_UA = "kairos-text-scouts/0.1 (+https://github.com/Kairos-cryptoAI)"


class RSSSource:
    name = "rss"

    def __init__(self, feeds: List[str], *, enabled: bool = True) -> None:
        self.feeds = feeds
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self.feeds)

    @staticmethod
    def _parse(xml: str, source: str) -> List[NewsItem]:
        items: List[NewsItem] = []
        try:
            root = ET.fromstring(xml)
        except (ET.ParseError, DefusedXmlException):
            return items
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            desc = _TAG.sub("", item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            if title:
                items.append(NewsItem(title=title, body=desc, url=link,
                                      source=source, source_kind="rss"))
        return items

    async def fetch(self) -> List[NewsItem]:  # pragma: no cover - network
        if aiohttp is None:
            return []
        out: List[NewsItem] = []
        async with aiohttp.ClientSession(headers={"User-Agent": _UA}) as session:
            for feed in self.feeds:
                try:
                    async with session.get(feed, timeout=10) as resp:
                        if resp.status == 200:
                            out.extend(self._parse(await resp.text(), feed))
                except Exception:
                    continue
        return out

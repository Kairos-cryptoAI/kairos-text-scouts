"""Minimal async RSS reader (Bloomberg / Reuters / Coindesk feeds).

Kept dependency-light: parses the handful of fields we need from the XML. For a
production deployment, swap in ``feedparser`` behind the same ``fetch`` method.
"""
from __future__ import annotations

import re
from typing import List
from xml.etree import ElementTree as ET

from ..models import NewsItem

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

_TAG = re.compile(r"<[^>]+>")


class RSSSource:
    def __init__(self, feeds: List[str]) -> None:
        self.feeds = feeds

    @staticmethod
    def _parse(xml: str, source: str) -> List[NewsItem]:
        items: List[NewsItem] = []
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            return items
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            desc = _TAG.sub("", item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            if title:
                items.append(NewsItem(title=title, body=desc, url=link, source=source))
        return items

    async def fetch(self) -> List[NewsItem]:  # pragma: no cover - network
        if aiohttp is None:
            return []
        out: List[NewsItem] = []
        async with aiohttp.ClientSession() as session:
            for feed in self.feeds:
                try:
                    async with session.get(feed, timeout=10) as resp:
                        if resp.status == 200:
                            out.extend(self._parse(await resp.text(), feed))
                except Exception:
                    continue
        return out

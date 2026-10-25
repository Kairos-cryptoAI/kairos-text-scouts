"""Minimal async RSS reader — the resilient fallback source.

GDELT is the primary news firehose; RSS stays as a cheap, dependency-light backstop
for a couple of crypto-native outlets (Coindesk, Cointelegraph). Reuters/Bloomberg
no longer publish public RSS, so we rely on GDELT for those. Parses only the handful
of fields we need; swap in ``feedparser`` behind the same ``fetch`` for production.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import aiohttp
from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException
from kairos_core.logging import get_logger

from ..models import NewsItem
from ..native_capture import (
    MAX_SOURCE_BODY_BYTES,
    NativeTextCaptureSink,
    checked_clock,
    emit_rss_response,
    utc_clock_ms,
)

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

    def __init__(
        self,
        feeds: list[str],
        *,
        enabled: bool = True,
        capture_sink: NativeTextCaptureSink | None = None,
        clock_ms: Callable[[], int] = utc_clock_ms,
    ) -> None:
        self.feeds = list(feeds)
        self._enabled = enabled
        self.capture_sink = capture_sink
        self._clock_ms = clock_ms

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
                captured = None
                try:
                    requested_at_ms = checked_clock(self._clock_ms())
                    async with session.get(
                        feed, timeout=_TIMEOUT, allow_redirects=self.capture_sink is None
                    ) as resp:
                        chunks = []
                        size = 0
                        async for chunk in resp.content.iter_chunked(65_536):
                            size += len(chunk)
                            if size > MAX_SOURCE_BODY_BYTES:
                                raise ValueError("RSS response exceeds the bounded native body limit")
                            chunks.append(chunk)
                        body = b"".join(chunks)
                        received_at_ms = checked_clock(self._clock_ms())
                        encoding = resp.charset or "utf-8"
                        items = self._parse(body.decode(encoding), feed) if resp.status == 200 else []
                        captured = (body, encoding, resp.status, requested_at_ms, received_at_ms, items)
                except (aiohttp.ClientError, TimeoutError, ValueError, LookupError) as exc:
                    log.warning("rss_fetch_failed", feed=feed, error=str(exc))
                # A persistence failure must not return parsed-but-unretained items.
                if captured is not None:
                    body, encoding, status, requested, received, items = captured
                    if self.capture_sink is not None:
                        emit_rss_response(
                            self.capture_sink,
                            source_name=feed,
                            body=body,
                            encoding=encoding,
                            status=status,
                            requested_at_ms=requested,
                            received_at_ms=received,
                            captured_at_ms=checked_clock(self._clock_ms()),
                            items=items,
                        )
                    out.extend(items)
        return out

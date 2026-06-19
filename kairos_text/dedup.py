"""Deduplicate events within a batch and across a rolling time window.

The same story reaches us many times: GDELT syndication, an RSS copy, and someone
tweeting the headline. We collapse them by a stable key (canonical URL, else a
normalized title) and remember keys for ``window_s`` so a story isn't re-sent to
the LLM every poll. The clock is injectable for deterministic tests.
"""
from __future__ import annotations

import re
import time
from typing import Callable, Dict, Iterable, List

from .models import NewsItem

_NON = re.compile(r"[^a-z0-9 ]+")
_WS = re.compile(r"\s+")


def normalize_title(title: str) -> str:
    return _WS.sub(" ", _NON.sub(" ", (title or "").lower())).strip()


def dedup_key(item: NewsItem) -> str:
    url = (item.url or "").split("?")[0].rstrip("/").lower()
    if url:
        return f"u:{url}"
    return f"t:{normalize_title(item.title)}"


class EventDeduplicator:
    def __init__(self, window_s: float = 21600.0, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.window_s = window_s
        self._seen: Dict[str, float] = {}
        self._clock = clock

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_s
        for key in [k for k, seen_at in self._seen.items() if seen_at < cutoff]:
            del self._seen[key]

    def filter_new(self, items: Iterable[NewsItem]) -> List[NewsItem]:
        now = self._clock()
        self._evict(now)
        fresh: List[NewsItem] = []
        batch_keys: set[str] = set()
        for item in items:
            key = dedup_key(item)
            if key in self._seen or key in batch_keys:
                continue
            batch_keys.add(key)
            fresh.append(item)
        for key in batch_keys:
            self._seen[key] = now
        return fresh

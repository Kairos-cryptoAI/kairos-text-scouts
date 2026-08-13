"""Deduplicate events within a batch and across a rolling time window.

The same story reaches us many times: GDELT syndication, an RSS copy, and someone
tweeting the headline. We collapse them by a stable key (canonical URL, else a
normalized title) and remember keys for ``window_s`` so a story isn't re-sent to
the LLM every poll. The clock is injectable for deterministic tests.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterable
from dataclasses import replace

from .models import NewsItem, canonical_url

_NON = re.compile(r"[^a-z0-9 ]+")
_WS = re.compile(r"\s+")


def normalize_title(title: str) -> str:
    return _WS.sub(" ", _NON.sub(" ", (title or "").lower())).strip()


def dedup_key(item: NewsItem) -> str:
    url = canonical_url(item.url)
    if url:
        return f"u:{url}"
    return f"t:{normalize_title(item.title)}"


def _quality_key(item: NewsItem) -> tuple[bool, int, int, float, float, str, str, str, str]:
    """Prefer exact timestamps and richer records, with deterministic ties."""
    return (
        not item.timestamp_is_estimated,
        len(item.body),
        len(item.title),
        item.engagement,
        item.published_at.timestamp(),
        item.source.casefold(),
        item.source_kind.casefold(),
        item.title.casefold(),
        item.body.casefold(),
    )


def _merge(left: NewsItem, right: NewsItem) -> NewsItem:
    winner = max((left, right), key=_quality_key)
    provenance = tuple(sorted(set(left.provenance_refs) | set(right.provenance_refs), key=str.casefold))
    return replace(winner, provenance=provenance)


class EventDeduplicator:
    def __init__(self, window_s: float = 21600.0, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.window_s = window_s
        self._seen: dict[str, float] = {}
        self._clock = clock

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_s
        for key in [k for k, seen_at in self._seen.items() if seen_at <= cutoff]:
            del self._seen[key]

    def collapse_batch(self, items: Iterable[NewsItem]) -> list[NewsItem]:
        """Merge duplicates without mutating rolling-window state."""
        batch: dict[str, NewsItem] = {}
        for item in items:
            key = dedup_key(item)
            batch[key] = _merge(batch[key], item) if key in batch else item
        return [batch[key] for key in sorted(batch)]

    def filter_unseen(self, items: Iterable[NewsItem]) -> list[NewsItem]:
        """Return rolling-window misses without consuming them."""
        now = self._clock()
        self._evict(now)
        return [item for item in self.collapse_batch(items) if dedup_key(item) not in self._seen]

    def remember(self, items: Iterable[NewsItem]) -> None:
        """Consume only items that actually advanced through the pipeline."""
        now = self._clock()
        self._evict(now)
        for item in items:
            self._seen[dedup_key(item)] = now

    def filter_new(self, items: Iterable[NewsItem]) -> list[NewsItem]:
        fresh = self.filter_unseen(items)
        self.remember(fresh)
        return fresh

"""Lightweight value objects for raw text items.

A ``NewsItem`` is the single normalized unit every source produces, regardless of
whether it came from a news aggregator (GDELT), an RSS feed, or a social API
(X / Reddit via Bright Data). ``source_kind`` records the provenance and
``engagement`` carries a coarse popularity signal (likes+reposts for X, score for
Reddit, ``0`` for news) so the relevance filter can weight loud social posts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref_src",
}


def canonical_url(value: str) -> str:
    """Return a stable URL identity without discarding semantic query fields."""
    raw = (value or "").strip()
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return raw
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return raw

    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in _TRACKING_QUERY_KEYS
    ]
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            urlencode(sorted(query)),
            "",
        )
    )


@dataclass(slots=True)
class NewsItem:
    title: str
    body: str = ""
    url: str = ""
    source: str = ""  # specific domain / feed / account
    source_kind: str = ""  # "gdelt" | "rss" | "x" | "reddit"
    engagement: float = 0.0  # likes+reposts (X), score (Reddit), 0 for news
    published_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    timestamp_is_estimated: bool = True
    provenance: tuple[str, ...] = ()

    @property
    def text(self) -> str:
        return f"{self.title}. {self.body}".strip()

    @property
    def provenance_refs(self) -> tuple[str, ...]:
        """Canonical evidence references suitable for ``SentimentSignal.sources``."""
        candidates = self.provenance or ((self.url or self.source),)
        refs = {
            canonical_url(candidate)
            for candidate in candidates
            if candidate and candidate.strip().lower() != "unknown"
        }
        return tuple(sorted(ref for ref in refs if ref))

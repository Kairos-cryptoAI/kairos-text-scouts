"""Lightweight value objects for raw text items.

A ``NewsItem`` is the single normalized unit every source produces, regardless of
whether it came from a news aggregator (GDELT), an RSS feed, or a social API
(X / Reddit via Bright Data). ``source_kind`` records the provenance and
``engagement`` carries a coarse popularity signal (likes+reposts for X, score for
Reddit, ``0`` for news) so the relevance filter can weight loud social posts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(slots=True)
class NewsItem:
    title: str
    body: str = ""
    url: str = ""
    source: str = ""              # specific domain / feed / account
    source_kind: str = ""         # "gdelt" | "rss" | "x" | "reddit"
    engagement: float = 0.0       # likes+reposts (X), score (Reddit), 0 for news
    published_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def text(self) -> str:
        return f"{self.title}. {self.body}".strip()

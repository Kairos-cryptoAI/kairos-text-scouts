"""Lightweight value objects for raw text items."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(slots=True)
class NewsItem:
    title: str
    body: str = ""
    url: str = ""
    source: str = ""
    published_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def text(self) -> str:
        return f"{self.title}. {self.body}".strip()

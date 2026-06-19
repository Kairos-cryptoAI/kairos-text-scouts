"""Common interface for every Text Scouts event source.

A source is anything that can asynchronously return a batch of ``NewsItem``s from
an *official* API or feed. We deliberately avoid self-hosted scrapers / proxies:
each source is either free-and-official (GDELT, RSS) or a managed API (Bright Data
for X / Reddit), which removes 403s, captchas, user-agent juggling and bans.
"""
from __future__ import annotations

from typing import List, Protocol, runtime_checkable

from ..models import NewsItem


@runtime_checkable
class EventSource(Protocol):
    """Duck-typed source contract used by :class:`~kairos_text.service.TextScoutsService`."""

    name: str

    @property
    def enabled(self) -> bool:
        """Whether this source is configured well enough to be polled."""
        ...

    async def fetch(self) -> List[NewsItem]:
        """Return the latest batch of items; must swallow its own network errors."""
        ...

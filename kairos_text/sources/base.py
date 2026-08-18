"""Common interface for every Text Scouts event source.

A source is anything that can asynchronously return a batch of ``NewsItem``s from
an *official* API or feed. We deliberately avoid self-hosted scrapers and proxies.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..models import NewsItem


@runtime_checkable
class EventSource(Protocol):
    """Duck-typed source contract used by :class:`~kairos_text.service.TextScoutsService`."""

    name: str

    @property
    def enabled(self) -> bool:
        """Whether this source is configured well enough to be polled."""
        ...

    async def fetch(self) -> list[NewsItem]:
        """Return the latest batch; the service isolates ordinary source errors."""
        ...


@runtime_checkable
class CommitAwareEventSource(EventSource, Protocol):
    """A source whose remote cursor advances only after the pipeline succeeds."""

    async def commit_fetch(self) -> None: ...

    async def abort_fetch(self) -> None: ...

"""Reddit source via the Bright Data Dataset API (token-gated).

Given a list of subreddits, returns recent posts as NewsItems with an ``engagement``
score (upvotes). Disabled automatically unless an API token + dataset id are set.
"""
from __future__ import annotations

from typing import Any, Dict, List

from ..models import NewsItem
from ._brightdata import API_BASE, collect, first, num, to_dt


class BrightDataRedditSource:
    name = "reddit"

    def __init__(self, *, token: str, dataset_id: str, subreddits: List[str],
                 poll_timeout_s: float = 90.0, base_url: str = API_BASE, enabled: bool = True) -> None:
        self.token = token
        self.dataset_id = dataset_id
        self.subreddits = subreddits
        self.poll_timeout_s = poll_timeout_s
        self.base_url = base_url
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return bool(self._enabled and self.token and self.dataset_id and self.subreddits)

    def _inputs(self) -> List[Dict[str, Any]]:
        return [{"url": f"https://www.reddit.com/r/{s.lstrip('r/').strip('/')}/"} for s in self.subreddits]

    @staticmethod
    def parse(records: List[Dict[str, Any]]) -> List[NewsItem]:
        out: List[NewsItem] = []
        for rec in records or []:
            title = str(first(rec, ["title", "post_title"])).strip()
            body = str(first(rec, ["description", "selftext", "text", "body"])).strip()
            if not title and not body:
                continue
            community = str(first(rec, ["community_name", "subreddit", "community"], "reddit"))
            out.append(NewsItem(
                title=(title or body[:120]),
                body=body,
                url=str(first(rec, ["url", "post_url"])),
                source=community,
                source_kind="reddit",
                engagement=num(rec, ["num_upvotes", "score", "upvotes"]),
                published_at=to_dt(first(rec, ["date_posted", "created_utc", "timestamp"], None)),
            ))
        return out

    async def fetch(self) -> List[NewsItem]:  # pragma: no cover - network
        records = await collect(token=self.token, dataset_id=self.dataset_id, inputs=self._inputs(),
                                poll_timeout_s=self.poll_timeout_s, base_url=self.base_url)
        return self.parse(records)

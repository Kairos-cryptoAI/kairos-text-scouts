"""Twitter/X source via the Bright Data Dataset API (token-gated).

Given a list of influencer handles, returns their latest posts as NewsItems with an
``engagement`` score (likes + reposts) so loud, high-signal posts survive the
relevance filter. Disabled automatically unless an API token + dataset id are set.
"""
from __future__ import annotations

from typing import Any, Dict, List

from ..models import NewsItem
from ._brightdata import API_BASE, collect, first, num, to_dt


class BrightDataXSource:
    name = "x"

    def __init__(self, *, token: str, dataset_id: str, accounts: List[str],
                 num_posts: int = 10, poll_timeout_s: float = 90.0,
                 base_url: str = API_BASE, enabled: bool = True) -> None:
        self.token = token
        self.dataset_id = dataset_id
        self.accounts = accounts
        self.num_posts = num_posts
        self.poll_timeout_s = poll_timeout_s
        self.base_url = base_url
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return bool(self._enabled and self.token and self.dataset_id and self.accounts)

    def _inputs(self) -> List[Dict[str, Any]]:
        return [{"url": f"https://x.com/{a.lstrip('@')}", "num_of_posts": self.num_posts}
                for a in self.accounts]

    @staticmethod
    def parse(records: List[Dict[str, Any]]) -> List[NewsItem]:
        out: List[NewsItem] = []
        for rec in records or []:
            text = str(first(rec, ["description", "text", "content", "post_text"])).strip()
            if not text:
                continue
            account = str(first(rec, ["user_posted", "profile_name", "name", "user_name"], "x"))
            out.append(NewsItem(
                title=text[:240],
                body=text,
                url=str(first(rec, ["url", "post_url"])),
                source=account,
                source_kind="x",
                engagement=num(rec, ["likes", "reposts", "retweets"]),
                published_at=to_dt(first(rec, ["date_posted", "timestamp", "date"], None)),
            ))
        return out

    async def fetch(self) -> List[NewsItem]:  # pragma: no cover - network
        records = await collect(token=self.token, dataset_id=self.dataset_id, inputs=self._inputs(),
                                poll_timeout_s=self.poll_timeout_s, base_url=self.base_url)
        return self.parse(records)

"""Reddit source via the **official Reddit API** (OAuth2, application-only).

Free and real-time: we obtain an app-only token with the client-credentials grant
(no user account needed, suitable for reading public listings), then pull each
subreddit's latest posts from ``oauth.reddit.com``. Each post becomes a NewsItem
with an ``engagement`` score (net upvotes). Disabled automatically unless a client
id + secret are configured.

Register an app at https://www.reddit.com/prefs/apps (type "script" or "web app")
to get the client id/secret. Network calls are ``pragma: no cover``; the listing
parser is pure and unit-tested.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from ..models import NewsItem

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
OAUTH_BASE = "https://oauth.reddit.com"


def _epoch_to_dt(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return datetime.now(timezone.utc)


class RedditSource:
    name = "reddit"

    def __init__(self, *, client_id: str, client_secret: str, user_agent: str,
                 subreddits: List[str], listing: str = "new", limit: int = 25,
                 oauth_base: str = OAUTH_BASE, token_url: str = TOKEN_URL,
                 enabled: bool = True) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.subreddits = subreddits
        self.listing = listing
        self.limit = limit
        self.oauth_base = oauth_base
        self.token_url = token_url
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return bool(self._enabled and self.client_id and self.client_secret and self.subreddits)

    @staticmethod
    def parse_listing(payload: Dict[str, Any]) -> List[NewsItem]:
        """Map a Reddit listing payload to NewsItems (pure, unit-tested)."""
        items: List[NewsItem] = []
        for child in (payload or {}).get("data", {}).get("children", []) or []:
            data = child.get("data", {}) or {}
            title = (data.get("title") or "").strip()
            if not title:
                continue
            permalink = data.get("permalink") or ""
            url = f"https://www.reddit.com{permalink}" if permalink else (data.get("url") or "")
            subreddit = data.get("subreddit") or ""
            items.append(NewsItem(
                title=title,
                body=(data.get("selftext") or "").strip(),
                url=url,
                source=f"r/{subreddit}" if subreddit else "reddit",
                source_kind="reddit",
                engagement=float(data.get("score") or 0),
                published_at=_epoch_to_dt(data.get("created_utc")),
            ))
        return items

    async def _access_token(  # pragma: no cover - network
        self, session: "aiohttp.ClientSession"
    ) -> str | None:
        auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
        async with session.post(self.token_url, auth=auth,
                                data={"grant_type": "client_credentials"},
                                headers={"User-Agent": self.user_agent}, timeout=15) as resp:
            if resp.status != 200:
                return None
            return (await resp.json()).get("access_token")

    async def fetch(self) -> List[NewsItem]:  # pragma: no cover - network
        if aiohttp is None:
            return []
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": self.user_agent}) as session:
                token = await self._access_token(session)
                if not token:
                    return []
                headers = {"Authorization": f"Bearer {token}", "User-Agent": self.user_agent}
                out: List[NewsItem] = []
                for sub in self.subreddits:
                    name = sub.removeprefix("r/").strip("/")
                    url = f"{self.oauth_base}/r/{name}/{self.listing}"
                    async with session.get(url, headers=headers, params={"limit": self.limit},
                                           timeout=15) as resp:
                        if resp.status != 200:
                            continue
                        out.extend(self.parse_listing(await resp.json()))
                return out
        except Exception:
            return []

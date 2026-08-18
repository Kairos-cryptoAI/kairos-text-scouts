"""Official X API v2 source with durable cursors and fail-closed spend control."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

import aiohttp
from kairos_persistence import SourceCursor

from ..models import NewsItem

X_API_BASE = "https://api.x.com"
REGISTERED_POST_READ_MICROUSD = 5_000
REGISTERED_USER_READ_MICROUSD = 10_000
REGISTERED_MONTHLY_BUDGET_MICROUSD = 10_000_000
_HANDLE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
_RESOURCE_ID = re.compile(r"^(0|[1-9][0-9]{0,19})$")
_TWEET_FIELDS = "author_id,conversation_id,created_at,entities,lang,public_metrics,referenced_tweets"


class XApiError(RuntimeError):
    """The official X API response could not be safely consumed."""


class SourceStateStore(Protocol):
    async def get_cursor(self, service: str, source: str, cursor_key: str) -> SourceCursor | None: ...

    async def advance_cursor(self, service: str, source: str, cursor_key: str, cursor_value: str) -> bool: ...

    async def reserve_usage(
        self,
        *,
        service: str,
        source: str,
        reservation_id: str,
        reserved_units: int,
        unit_cost_microusd: int,
        monthly_budget_microusd: int,
        requested_at: datetime | None = None,
    ) -> object: ...

    async def commit_usage(
        self, service: str, source: str, reservation_id: str, actual_units: int
    ) -> object: ...

    async def release_usage(self, service: str, source: str, reservation_id: str) -> object: ...


@dataclass(frozen=True, slots=True)
class XApiResponse:
    status: int
    payload: dict[str, Any]
    headers: Mapping[str, str]


Requester = Callable[[str, Mapping[str, str]], Awaitable[XApiResponse]]


class XApiSource:
    """Poll selected public accounts through immutable, durably cached User IDs."""

    name = "x"

    def __init__(
        self,
        *,
        bearer_token: str,
        accounts: list[str],
        service_name: str = "kairos-text-scouts",
        max_results: int = 10,
        max_pages: int = 3,
        timeout_s: float = 30.0,
        monthly_budget_microusd: int = 10_000_000,
        post_read_unit_cost_microusd: int = 5_000,
        user_read_unit_cost_microusd: int = 10_000,
        state: SourceStateStore | None = None,
        requester: Requester | None = None,
        enabled: bool = True,
    ) -> None:
        if not service_name.strip():
            raise ValueError("service_name must not be empty")
        if isinstance(max_results, bool) or not isinstance(max_results, int) or not 5 <= max_results <= 100:
            raise ValueError("max_results must be between 5 and 100")
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or not 1 <= max_pages <= 10:
            raise ValueError("max_pages must be between 1 and 10")
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise ValueError("timeout_s must be positive")
        if (
            isinstance(monthly_budget_microusd, bool)
            or not isinstance(monthly_budget_microusd, int)
            or monthly_budget_microusd < 0
            or monthly_budget_microusd > REGISTERED_MONTHLY_BUDGET_MICROUSD
            or isinstance(post_read_unit_cost_microusd, bool)
            or not isinstance(post_read_unit_cost_microusd, int)
            or post_read_unit_cost_microusd < REGISTERED_POST_READ_MICROUSD
            or isinstance(user_read_unit_cost_microusd, bool)
            or not isinstance(user_read_unit_cost_microusd, int)
            or user_read_unit_cost_microusd < REGISTERED_USER_READ_MICROUSD
        ):
            raise ValueError("X costs violate the registered price floor or monthly budget ceiling")
        normalized_accounts = tuple(self._handle(item) for item in accounts)
        if len(set(normalized_accounts)) != len(normalized_accounts):
            raise ValueError("X accounts must be unique after normalization")
        self.bearer_token = bearer_token.strip()
        self.accounts = normalized_accounts
        self.service_name = service_name.strip()
        self.max_results = max_results
        self.max_pages = max_pages
        self.timeout_s = timeout_s
        self.monthly_budget_microusd = monthly_budget_microusd
        self.post_read_unit_cost_microusd = post_read_unit_cost_microusd
        self.user_read_unit_cost_microusd = user_read_unit_cost_microusd
        self.state = state
        self._requester = requester or self._request
        self._enabled = enabled
        self._pending_cursors: dict[str, str] = {}
        self._fetch_pending = False
        self.rate_limit_observed = False

    @property
    def enabled(self) -> bool:
        return bool(self._enabled and self.bearer_token and self.accounts)

    @property
    def state_attached(self) -> bool:
        return self.state is not None

    def attach_state(self, state: SourceStateStore) -> None:
        if self.state is not None and self.state is not state:
            raise RuntimeError("X source state is already attached")
        self.state = state

    async def fetch(self) -> list[NewsItem]:
        if not self.enabled:
            return []
        if self.state is None:
            raise RuntimeError("official X polling requires a durable source state repository")
        if self._fetch_pending:
            raise RuntimeError("previous X fetch must be committed or aborted before polling again")

        items: list[NewsItem] = []
        pending: dict[str, str] = {}
        try:
            for account in self.accounts:
                account_items, newest_id = await self._fetch_account(account)
                items.extend(account_items)
                if newest_id is not None:
                    pending[account] = newest_id
        except BaseException:
            self._pending_cursors.clear()
            self._fetch_pending = False
            raise
        self._pending_cursors = pending
        self._fetch_pending = True
        return sorted(items, key=lambda item: (item.published_at, item.url))

    async def commit_fetch(self) -> None:
        if self.state is None:
            raise RuntimeError("official X polling requires a durable source state repository")
        for account, cursor in sorted(self._pending_cursors.items()):
            await self.state.advance_cursor(
                self.service_name,
                self.name,
                account,
                cursor,
            )
        self._pending_cursors.clear()
        self._fetch_pending = False

    async def abort_fetch(self) -> None:
        self._pending_cursors.clear()
        self._fetch_pending = False

    async def _fetch_account(self, account: str) -> tuple[list[NewsItem], str | None]:
        if self.state is None:
            raise RuntimeError("official X polling requires a durable source state repository")
        user_id = await self._resolve_user_id(account)
        cursor = await self.state.get_cursor(self.service_name, self.name, account)
        since_id = None if cursor is None else cursor.cursor_value
        pagination_token: str | None = None
        items: list[NewsItem] = []
        ids: list[int] = []
        for page in range(self.max_pages):
            params = {
                "max_results": str(self.max_results),
                "exclude": "replies,retweets",
                "tweet.fields": _TWEET_FIELDS,
            }
            if since_id is not None:
                params["since_id"] = since_id
            if pagination_token is not None:
                params["pagination_token"] = pagination_token
            response = await self._metered_post_request(account, user_id, params)
            self._observe_rate_limit(response.headers)
            page_items, page_ids, pagination_token = self._parse_page(account, response.payload)
            if len(set(page_ids)) != len(page_ids) or set(page_ids).intersection(ids):
                raise XApiError(f"X API returned duplicate Post IDs for @{account}")
            if since_id is not None and any(post_id <= int(since_id) for post_id in page_ids):
                raise XApiError(f"X API violated the since_id boundary for @{account}")
            items.extend(page_items)
            ids.extend(page_ids)
            # Initial registration deliberately starts from the newest bounded
            # page. Historical pagination would spend budget on stale content.
            if since_id is None or pagination_token is None:
                break
            if page == self.max_pages - 1:
                raise XApiError(
                    f"X backlog for @{account} exceeded the configured {self.max_pages}-page bound"
                )
        return items, None if not ids else str(max(ids))

    async def _resolve_user_id(self, account: str) -> str:
        if self.state is None:
            raise RuntimeError("official X polling requires a durable source state repository")
        cursor_key = f"user-id:{account}"
        cached = await self.state.get_cursor(self.service_name, self.name, cursor_key)
        if cached is not None:
            return self._resource_id(cached.cursor_value, "User")

        reservation_id = f"user:{account}:{uuid4().hex}"
        await self.state.reserve_usage(
            service=self.service_name,
            source=self.name,
            reservation_id=reservation_id,
            reserved_units=1,
            unit_cost_microusd=self.user_read_unit_cost_microusd,
            monthly_budget_microusd=self.monthly_budget_microusd,
            requested_at=datetime.now(UTC),
        )
        response = await self._requester(f"/2/users/by/username/{account}", {})
        self._observe_rate_limit(response.headers)
        if response.status != 200:
            await self.state.release_usage(self.service_name, self.name, reservation_id)
            raise XApiError(f"X API User lookup failed with HTTP {response.status}")
        try:
            user_id = self._parse_user_id(account, response.payload)
        except XApiError:
            # A successful HTTP response may already be billable even when its
            # payload is unusable, so account for the one returned resource.
            await self.state.commit_usage(self.service_name, self.name, reservation_id, 1)
            raise
        await self.state.commit_usage(self.service_name, self.name, reservation_id, 1)
        await self.state.advance_cursor(
            self.service_name,
            self.name,
            cursor_key,
            user_id,
        )
        return user_id

    async def _metered_post_request(
        self, account: str, user_id: str, params: Mapping[str, str]
    ) -> XApiResponse:
        if self.state is None:
            raise RuntimeError("official X polling requires a durable source state repository")
        reservation_id = f"{account}:{uuid4().hex}"
        await self.state.reserve_usage(
            service=self.service_name,
            source=self.name,
            reservation_id=reservation_id,
            reserved_units=self.max_results,
            unit_cost_microusd=self.post_read_unit_cost_microusd,
            monthly_budget_microusd=self.monthly_budget_microusd,
            requested_at=datetime.now(UTC),
        )
        # A transport exception after dispatch has ambiguous billing semantics.
        # Preserve the reservation (fail closed) until an operator reconciles it.
        response = await self._requester(f"/2/users/{user_id}/tweets", params)
        if response.status != 200:
            await self.state.release_usage(self.service_name, self.name, reservation_id)
            raise XApiError(f"X API request failed with HTTP {response.status}")
        raw_data = response.payload.get("data", [])
        if not isinstance(raw_data, list):
            await self.state.commit_usage(
                self.service_name,
                self.name,
                reservation_id,
                self.max_results,
            )
            raise XApiError("X API returned a malformed data collection")
        await self.state.commit_usage(
            self.service_name,
            self.name,
            reservation_id,
            len(raw_data),
        )
        return response

    async def _request(self, endpoint: str, params: Mapping[str, str]) -> XApiResponse:
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": "application/json",
            "User-Agent": "kairos-text-scouts/0.1",
        }
        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        if not endpoint.startswith("/2/") or ".." in endpoint:
            raise XApiError("X API endpoint is outside the registered v2 boundary")
        url = f"{X_API_BASE}{endpoint}"
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url, params=dict(params)) as response:
                body = await response.read()
                if len(body) > 1_000_000:
                    raise XApiError("X API response exceeded the 1 MB safety bound")
                try:
                    payload = json.loads(body) if body else {}
                except json.JSONDecodeError as exc:
                    if response.status != 200:
                        payload = {}
                    else:
                        raise XApiError("X API returned invalid JSON with HTTP 200") from exc
                if not isinstance(payload, dict):
                    raise XApiError("X API response root must be an object")
                return XApiResponse(
                    status=response.status,
                    payload=payload,
                    headers={key.lower(): value for key, value in response.headers.items()},
                )

    @classmethod
    def _parse_page(
        cls, account: str, payload: dict[str, Any]
    ) -> tuple[list[NewsItem], list[int], str | None]:
        errors = payload.get("errors")
        if errors:
            raise XApiError(f"X API returned an account-level error for @{account}")
        data = payload.get("data", [])
        if not isinstance(data, list):
            raise XApiError("X API data must be a list")
        items: list[NewsItem] = []
        ids: list[int] = []
        for raw in data:
            if not isinstance(raw, dict):
                raise XApiError("X API post must be an object")
            post_id = cls._post_id(raw.get("id"))
            text = raw.get("text")
            if not isinstance(text, str) or not text.strip():
                raise XApiError("X API post has no text")
            created_at = cls._created_at(raw.get("created_at"))
            metrics = raw.get("public_metrics", {})
            if not isinstance(metrics, dict):
                raise XApiError("X API public_metrics must be an object")
            engagement = sum(
                cls._metric(metrics, name) for name in ("like_count", "retweet_count", "quote_count")
            )
            url = f"https://x.com/{account}/status/{post_id}"
            items.append(
                NewsItem(
                    title=text.strip()[:240],
                    body=text.strip(),
                    url=url,
                    source=account,
                    source_kind="x",
                    engagement=float(engagement),
                    published_at=created_at,
                    timestamp_is_estimated=False,
                    provenance=(url,),
                )
            )
            ids.append(int(post_id))
        meta = payload.get("meta", {})
        if not isinstance(meta, dict):
            raise XApiError("X API meta must be an object")
        result_count = meta.get("result_count")
        if result_count is not None and (
            isinstance(result_count, bool) or not isinstance(result_count, int) or result_count != len(data)
        ):
            raise XApiError("X API result_count does not match the returned Posts")
        token = meta.get("next_token")
        if token is not None and (not isinstance(token, str) or not token.strip()):
            raise XApiError("X API pagination token is malformed")
        return items, ids, token

    @classmethod
    def _parse_user_id(cls, account: str, payload: dict[str, Any]) -> str:
        errors = payload.get("errors")
        if errors:
            raise XApiError(f"X API returned a User lookup error for @{account}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise XApiError("X API User lookup data must be an object")
        username = data.get("username")
        if not isinstance(username, str) or username.casefold() != account.casefold():
            raise XApiError("X API User lookup identity does not match the requested account")
        return cls._resource_id(data.get("id"), "User")

    @staticmethod
    def _handle(value: str) -> str:
        if not isinstance(value, str):
            raise ValueError("X account must be a string")
        handle = value.strip().removeprefix("@").lower()
        if not _HANDLE.fullmatch(handle):
            raise ValueError(f"invalid X account handle: {value!r}")
        return handle

    @staticmethod
    def _post_id(value: Any) -> str:
        return XApiSource._resource_id(value, "Post")

    @staticmethod
    def _resource_id(value: Any, kind: str) -> str:
        if not isinstance(value, str) or not _RESOURCE_ID.fullmatch(value):
            raise XApiError(f"X API {kind} ID must be a canonical unsigned decimal string")
        if int(value) > 2**64 - 1:
            raise XApiError(f"X API {kind} ID exceeds unsigned 64-bit range")
        return value

    @staticmethod
    def _created_at(value: Any) -> datetime:
        if not isinstance(value, str):
            raise XApiError("X API post has no publication timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise XApiError("X API post timestamp is invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise XApiError("X API post timestamp must be timezone-aware")
        return parsed.astimezone(UTC)

    @staticmethod
    def _metric(metrics: dict[str, Any], name: str) -> int:
        value = metrics.get(name, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise XApiError(f"X API metric {name} must be a non-negative integer")
        return value

    def _observe_rate_limit(self, headers: Mapping[str, str]) -> None:
        normalized = {key.lower(): value for key, value in headers.items()}
        names = ("x-rate-limit-limit", "x-rate-limit-remaining", "x-rate-limit-reset")
        try:
            observed = all(int(normalized[name]) >= 0 for name in names)
        except (KeyError, TypeError, ValueError):
            observed = False
        self.rate_limit_observed = self.rate_limit_observed or observed

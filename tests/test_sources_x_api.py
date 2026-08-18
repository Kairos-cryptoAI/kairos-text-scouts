from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from kairos_persistence import SourceBudgetExceeded, SourceCursor

from kairos_text.sources.x_api import XApiError, XApiResponse, XApiSource

RATE_HEADERS = {
    "x-rate-limit-limit": "1500",
    "x-rate-limit-remaining": "1499",
    "x-rate-limit-reset": "1787070000",
}


class _State:
    def __init__(self, *, budget_microusd: int = 10_000_000) -> None:
        self.cursors: dict[tuple[str, str, str], str] = {
            ("kairos-text-scouts", "x", "user-id:lookonchain"): "42"
        }
        self.reservations: dict[str, tuple[int, int, str, int | None]] = {}
        self.budget_microusd = budget_microusd

    async def get_cursor(self, service, source, cursor_key):
        value = self.cursors.get((service, source, cursor_key))
        return None if value is None else SourceCursor(service, source, cursor_key, value, datetime.now(UTC))

    async def advance_cursor(self, service, source, cursor_key, cursor_value):
        key = (service, source, cursor_key)
        previous = self.cursors.get(key)
        if previous is not None and int(cursor_value) < int(previous):
            raise ValueError("cursor regression")
        self.cursors[key] = cursor_value
        return previous != cursor_value

    async def reserve_usage(
        self,
        *,
        service,
        source,
        reservation_id,
        reserved_units,
        unit_cost_microusd,
        monthly_budget_microusd,
        requested_at=None,
    ):
        del service, source, requested_at
        budgeted = sum(
            (actual if status == "COMMITTED" else units) * unit_cost
            for units, unit_cost, status, actual in self.reservations.values()
            if status != "RELEASED"
        )
        cost = reserved_units * unit_cost_microusd
        if budgeted + cost > min(self.budget_microusd, monthly_budget_microusd):
            raise SourceBudgetExceeded("test budget exceeded")
        self.reservations[reservation_id] = (
            reserved_units,
            unit_cost_microusd,
            "RESERVED",
            None,
        )
        return object()

    async def commit_usage(self, service, source, reservation_id, actual_units):
        del service, source
        reserved, unit_cost, _status, _actual = self.reservations[reservation_id]
        assert actual_units <= reserved
        self.reservations[reservation_id] = (
            reserved,
            unit_cost,
            "COMMITTED",
            actual_units,
        )
        return object()

    async def release_usage(self, service, source, reservation_id):
        del service, source
        reserved, unit_cost, _status, _actual = self.reservations[reservation_id]
        self.reservations[reservation_id] = (reserved, unit_cost, "RELEASED", None)
        return object()


def _post(post_id: str, *, text: str = "BTC liquidity update") -> dict[str, Any]:
    return {
        "id": post_id,
        "text": text,
        "created_at": "2026-08-18T12:30:00Z",
        "public_metrics": {
            "like_count": 10,
            "retweet_count": 3,
            "quote_count": 2,
            "reply_count": 99,
        },
    }


def test_parse_maps_official_fields_and_uses_attributable_url() -> None:
    items, ids, token = XApiSource._parse_page(
        "lookonchain",
        {"data": [_post("100")], "meta": {}},
    )

    assert ids == [100]
    assert token is None
    assert items[0].source == "lookonchain"
    assert items[0].source_kind == "x"
    assert items[0].engagement == 15.0
    assert items[0].url == "https://x.com/lookonchain/status/100"
    assert items[0].provenance == (items[0].url,)
    assert items[0].published_at == datetime(2026, 8, 18, 12, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "payload",
    [
        {"data": [{**_post("01")}], "meta": {}},
        {"data": [{**_post("1"), "created_at": "not-a-date"}], "meta": {}},
        {"data": [{**_post("1"), "public_metrics": {"like_count": -1}}], "meta": {}},
        {"data": [_post("1")], "meta": {"next_token": ""}},
        {"data": [_post("1")], "meta": {"result_count": 2}},
        {"data": [_post("1")], "errors": [{"detail": "withheld"}]},
    ],
)
def test_parse_fails_closed_on_noncanonical_evidence(payload) -> None:
    with pytest.raises(XApiError):
        XApiSource._parse_page("lookonchain", payload)


@pytest.mark.asyncio
async def test_fetch_uses_since_id_pages_and_advances_only_after_commit() -> None:
    state = _State()
    state.cursors[("text", "x", "user-id:lookonchain")] = "42"
    state.cursors[("text", "x", "lookonchain")] = "99"
    calls: list[dict[str, str]] = []

    async def requester(_account, params):
        calls.append(dict(params))
        if len(calls) == 1:
            return XApiResponse(
                200,
                {"data": [_post("101")], "meta": {"next_token": "next"}},
                RATE_HEADERS,
            )
        return XApiResponse(200, {"data": [_post("100")], "meta": {}}, RATE_HEADERS)

    source = XApiSource(
        bearer_token="secret",
        accounts=["@LookOnChain"],
        service_name="text",
        state=state,
        requester=requester,
    )

    items = await source.fetch()

    assert [item.url.rsplit("/", 1)[-1] for item in items] == ["100", "101"]
    assert calls[0]["since_id"] == "99"
    assert calls[1]["pagination_token"] == "next"
    assert state.cursors[("text", "x", "lookonchain")] == "99"
    assert source.rate_limit_observed
    await source.commit_fetch()
    assert state.cursors[("text", "x", "lookonchain")] == "101"
    assert [row[3] for row in state.reservations.values()] == [1, 1]


@pytest.mark.asyncio
async def test_user_id_is_resolved_once_metered_and_cached_durably() -> None:
    state = _State()
    del state.cursors[("kairos-text-scouts", "x", "user-id:lookonchain")]
    endpoints: list[str] = []
    timeline_calls = 0

    async def requester(endpoint, _params):
        nonlocal timeline_calls
        endpoints.append(endpoint)
        if endpoint == "/2/users/by/username/lookonchain":
            return XApiResponse(
                200,
                {"data": {"id": "42", "username": "LookOnChain"}, "meta": {}},
                RATE_HEADERS,
            )
        assert endpoint == "/2/users/42/tweets"
        timeline_calls += 1
        if timeline_calls > 1:
            return XApiResponse(200, {"data": [], "meta": {}}, RATE_HEADERS)
        return XApiResponse(200, {"data": [_post("100")], "meta": {}}, RATE_HEADERS)

    source = XApiSource(
        bearer_token="secret",
        accounts=["lookonchain"],
        state=state,
        requester=requester,
    )
    assert len(await source.fetch()) == 1
    assert state.cursors[(source.service_name, "x", "user-id:lookonchain")] == "42"
    assert [row[1] for row in state.reservations.values()] == [10_000, 5_000]
    assert [row[3] for row in state.reservations.values()] == [1, 1]
    await source.commit_fetch()

    assert len(await source.fetch()) == 0
    assert endpoints.count("/2/users/by/username/lookonchain") == 1


@pytest.mark.asyncio
async def test_failed_user_lookup_releases_budget_and_never_reads_posts() -> None:
    state = _State()
    del state.cursors[("kairos-text-scouts", "x", "user-id:lookonchain")]
    endpoints: list[str] = []

    async def requester(endpoint, _params):
        endpoints.append(endpoint)
        return XApiResponse(402, {}, {})

    source = XApiSource(
        bearer_token="secret",
        accounts=["lookonchain"],
        state=state,
        requester=requester,
    )
    with pytest.raises(XApiError, match="User lookup failed with HTTP 402"):
        await source.fetch()
    assert endpoints == ["/2/users/by/username/lookonchain"]
    assert {row[2] for row in state.reservations.values()} == {"RELEASED"}


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"id": "01", "username": "lookonchain"}},
        {"data": {"id": "42", "username": "different"}},
        {"data": []},
        {"errors": [{"detail": "withheld"}]},
    ],
)
def test_user_lookup_requires_canonical_matching_identity(payload) -> None:
    with pytest.raises(XApiError):
        XApiSource._parse_user_id("lookonchain", payload)


@pytest.mark.asyncio
async def test_failed_request_is_not_charged_and_releases_reservation() -> None:
    state = _State()

    async def requester(_account, _params):
        return XApiResponse(429, {"title": "rate limit"}, RATE_HEADERS)

    source = XApiSource(
        bearer_token="secret",
        accounts=["lookonchain"],
        state=state,
        requester=requester,
    )
    with pytest.raises(XApiError, match="429"):
        await source.fetch()
    assert {row[2] for row in state.reservations.values()} == {"RELEASED"}


@pytest.mark.asyncio
async def test_ambiguous_transport_failure_preserves_reserved_budget() -> None:
    state = _State()

    async def requester(_account, _params):
        raise TimeoutError("response lost after dispatch")

    source = XApiSource(
        bearer_token="secret",
        accounts=["lookonchain"],
        state=state,
        requester=requester,
    )
    with pytest.raises(TimeoutError):
        await source.fetch()
    assert {row[2] for row in state.reservations.values()} == {"RESERVED"}


@pytest.mark.asyncio
async def test_initial_registration_reads_only_newest_page() -> None:
    state = _State()
    calls = 0

    async def requester(_account, _params):
        nonlocal calls
        calls += 1
        return XApiResponse(
            200,
            {"data": [_post("100")], "meta": {"next_token": "historical"}},
            RATE_HEADERS,
        )

    source = XApiSource(
        bearer_token="secret",
        accounts=["lookonchain"],
        state=state,
        requester=requester,
    )
    assert len(await source.fetch()) == 1
    assert calls == 1
    await source.commit_fetch()
    assert state.cursors[(source.service_name, "x", "lookonchain")] == "100"


@pytest.mark.asyncio
async def test_budget_is_reserved_before_network_and_pending_fetch_cannot_be_overwritten() -> None:
    state = _State(budget_microusd=49_999)
    network_called = False

    async def requester(_account, _params):
        nonlocal network_called
        network_called = True
        return XApiResponse(200, {"data": [], "meta": {}}, RATE_HEADERS)

    source = XApiSource(
        bearer_token="secret",
        accounts=["lookonchain"],
        state=state,
        requester=requester,
    )
    with pytest.raises(SourceBudgetExceeded):
        await source.fetch()
    assert not network_called

    state.budget_microusd = 10_000_000
    await source.fetch()
    with pytest.raises(RuntimeError, match="committed or aborted"):
        await source.fetch()
    await source.abort_fetch()
    assert await source.fetch() == []


def test_configuration_rejects_aliases_and_invalid_handles() -> None:
    with pytest.raises(ValueError, match="unique"):
        XApiSource(bearer_token="x", accounts=["LookOnChain", "@lookonchain"])
    with pytest.raises(ValueError, match="invalid"):
        XApiSource(bearer_token="x", accounts=["bad-handle"])
    with pytest.raises(ValueError, match="price floor"):
        XApiSource(
            bearer_token="x",
            accounts=["lookonchain"],
            post_read_unit_cost_microusd=4_999,
        )
    with pytest.raises(ValueError, match="price floor"):
        XApiSource(
            bearer_token="x",
            accounts=["lookonchain"],
            user_read_unit_cost_microusd=9_999,
        )
    with pytest.raises(ValueError, match="budget ceiling"):
        XApiSource(
            bearer_token="x",
            accounts=["lookonchain"],
            monthly_budget_microusd=10_000_001,
        )
    assert XApiSource(bearer_token="", accounts=["lookonchain"]).enabled is False

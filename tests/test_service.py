"""Service wiring + end-to-end pipeline on the in-memory bus (no network, no keys)."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kairos_llm import BudgetedLLMGateway, DenyLLMUsageBudget, LLMWorkload
from kairos_persistence import DurableLLMUsageBudget

from kairos_text.config import TextSettings
from kairos_text.models import NewsItem
from kairos_text.service import TextScoutsService


def test_gateway_health_hook_is_wired():
    svc = TextScoutsService(TextSettings(bus_backend="memory"))
    assert isinstance(svc.extractor.gateway, BudgetedLLMGateway)
    assert isinstance(svc.extractor.gateway.budget, DenyLLMUsageBudget)
    assert svc.extractor.gateway._on_health is not None


def test_durable_runtime_wires_shared_provider_budget():
    svc = TextScoutsService(TextSettings(bus_backend="redis"))
    assert isinstance(svc.extractor.gateway.budget, DurableLLMUsageBudget)


def test_x_bearer_token_is_redacted_by_settings_repr():
    settings = TextSettings(x_bearer_token="not-for-logs")
    assert "not-for-logs" not in repr(settings)


class _StubSource:
    name = "stub"
    enabled = True

    def __init__(self, items):
        self._items = items

    async def fetch(self):
        return list(self._items)


class _CommitAwareSource(_StubSource):
    def __init__(self, items):
        super().__init__(items)
        self.commits = 0
        self.aborts = 0

    async def commit_fetch(self):
        self.commits += 1

    async def abort_fetch(self):
        self.aborts += 1


class _FakeGateway:
    async def complete(self, *, system, user, workload, schema=None):
        assert workload is LLMWorkload.TEXT_SCOUTS
        return SimpleNamespace(
            parsed=schema.model_validate(
                {
                    "signals": [
                        {
                            "topic": "SEC ETF",
                            "sentiment": 0.8,
                            "impact": "bullish",
                            "confidence": 0.9,
                            "summary": "approval",
                            "item_ids": [1],
                        }
                    ]
                }
            )
        )


class _CancelledSource:
    name = "cancelled"
    enabled = True

    async def fetch(self):
        raise asyncio.CancelledError


class _FailingBus:
    def __init__(self, *, fail_after=0):
        self.fail_after = fail_after
        self.published = []

    async def publish(self, topic, message):
        if len(self.published) >= self.fail_after:
            raise RuntimeError("bus unavailable")
        self.published.append(message)


class _AbstainingGateway:
    async def complete(self, *, system, user, workload, schema=None):
        return SimpleNamespace(parsed=schema.model_validate({"signals": []}))


class _PartiallyAbstainingGateway:
    async def complete(self, *, system, user, workload, schema=None):
        return SimpleNamespace(
            parsed=schema.model_validate(
                {
                    "signals": [
                        {
                            "topic": "first",
                            "sentiment": 0.8,
                            "impact": "bullish",
                            "confidence": 0.8,
                            "summary": "supported",
                            "item_ids": [1],
                        }
                    ]
                }
            )
        )


def test_poll_once_aggregates_dedups_filters_and_publishes():
    now = datetime.now(UTC)
    items = [
        NewsItem(
            title="SEC approves spot Bitcoin ETF",
            url="https://a/1",
            source_kind="gdelt",
            published_at=now,
            timestamp_is_estimated=False,
        ),
        NewsItem(
            title="SEC approves spot Bitcoin ETF",
            url="https://a/1",
            source_kind="rss",
            published_at=now,
            timestamp_is_estimated=False,
        ),  # duplicate
        NewsItem(
            title="Local bakery wins a small award",
            url="https://a/2",
            source_kind="rss",
            published_at=now,
            timestamp_is_estimated=False,
        ),  # noise
    ]
    svc = TextScoutsService(
        TextSettings(bus_backend="memory"), gateway=_FakeGateway(), sources=[_StubSource(items)]
    )
    published = asyncio.run(svc.poll_once())
    assert published == 1  # dup collapsed, noise filtered, one signal emitted


def test_gather_propagates_source_cancellation():
    svc = TextScoutsService(
        TextSettings(bus_backend="memory"), gateway=_FakeGateway(), sources=[_CancelledSource()]
    )
    try:
        asyncio.run(svc._gather())
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("source cancellation must propagate to stop the service")


def test_publish_failure_does_not_consume_dedup_state():
    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC approves spot Bitcoin ETF",
        url="https://a/1",
        published_at=now,
        timestamp_is_estimated=False,
    )
    svc = TextScoutsService(
        TextSettings(bus_backend="memory"), gateway=_FakeGateway(), sources=[_StubSource([item])]
    )
    svc.bus = _FailingBus()
    try:
        asyncio.run(svc.poll_once())
    except RuntimeError:
        pass
    else:
        raise AssertionError("publish failure must propagate")
    assert svc.dedup.filter_unseen([item]) == [item]


def test_paid_source_cursor_commits_only_after_pipeline_success():
    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC approves spot Bitcoin ETF",
        url="https://x.com/source/status/1",
        source="source",
        source_kind="x",
        published_at=now,
        timestamp_is_estimated=False,
    )
    source = _CommitAwareSource([item])
    service = TextScoutsService(TextSettings(bus_backend="memory"), gateway=_FakeGateway(), sources=[source])
    assert asyncio.run(service.poll_once()) == 1
    assert source.commits == 1
    assert source.aborts == 0

    failing_source = _CommitAwareSource([item])
    failing = TextScoutsService(
        TextSettings(bus_backend="memory"), gateway=_FakeGateway(), sources=[failing_source]
    )
    failing.bus = _FailingBus()
    with pytest.raises(RuntimeError, match="bus unavailable"):
        asyncio.run(failing.poll_once())
    assert failing_source.commits == 0
    assert failing_source.aborts == 1


def test_official_x_refuses_paid_polling_without_durable_state():
    service = TextScoutsService(
        TextSettings(bus_backend="memory", x_bearer_token="configured"), gateway=_FakeGateway()
    )
    with pytest.raises(RuntimeError, match="durable PostgreSQL"):
        asyncio.run(service.poll_once())


def test_partial_publish_retry_reuses_exact_signal_id_and_event_time():
    now = datetime(2026, 8, 13, 9, tzinfo=UTC)
    items = [
        NewsItem(
            title="Bitcoin exchange hack",
            url="https://a/1",
            published_at=now,
            timestamp_is_estimated=False,
        ),
        NewsItem(
            title="Bitcoin ETF approval rally",
            url="https://a/2",
            published_at=now,
            timestamp_is_estimated=False,
        ),
    ]
    gateway_response = {
        "signals": [
            {
                "topic": "Alpha",
                "sentiment": 0.8,
                "impact": "bullish",
                "confidence": 0.8,
                "summary": "supported",
                "item_ids": [1],
            },
            {
                "topic": "Zulu",
                "sentiment": -0.8,
                "impact": "bearish",
                "confidence": 0.8,
                "summary": "supported",
                "item_ids": [2],
            },
        ]
    }

    class _ReplayGateway:
        async def complete(self, *, system, user, workload, schema=None):
            return SimpleNamespace(parsed=schema.model_validate(gateway_response))

    svc = TextScoutsService(
        TextSettings(bus_backend="memory", top_k=2),
        gateway=_ReplayGateway(),
        sources=[_StubSource(items)],
        clock=lambda: now,
    )
    failing_bus = _FailingBus(fail_after=1)
    svc.bus = failing_bus
    try:
        asyncio.run(svc.poll_once())
    except RuntimeError:
        pass
    first_attempt = failing_bus.published[0]

    retry_bus = _FailingBus(fail_after=10)
    svc.bus = retry_bus
    assert asyncio.run(svc.poll_once()) == 2
    replay = next(signal for signal in retry_bus.published if signal.topic == first_attempt.topic)
    assert replay.message_id == first_attempt.message_id
    assert replay.produced_at == first_attempt.produced_at == now


def test_abstention_does_not_consume_dedup_state():
    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC Bitcoin ETF hearing",
        url="https://a/1",
        published_at=now,
        timestamp_is_estimated=False,
    )
    svc = TextScoutsService(
        TextSettings(bus_backend="memory"), gateway=_AbstainingGateway(), sources=[_StubSource([item])]
    )
    assert asyncio.run(svc.poll_once()) == 0
    assert svc.dedup.filter_unseen([item]) == [item]


def test_only_evidence_backing_a_published_signal_is_remembered():
    now = datetime.now(UTC)
    items = [
        NewsItem(
            title="Bitcoin exchange hack",
            url="https://a/1",
            published_at=now,
            timestamp_is_estimated=False,
        ),
        NewsItem(
            title="Bitcoin ETF approval rally",
            url="https://a/2",
            published_at=now,
            timestamp_is_estimated=False,
        ),
    ]
    svc = TextScoutsService(
        TextSettings(bus_backend="memory", top_k=2),
        gateway=_PartiallyAbstainingGateway(),
        sources=[_StubSource(items)],
    )
    assert asyncio.run(svc.poll_once()) == 1
    unseen = svc.dedup.filter_unseen(items)
    assert [item.url for item in unseen] == ["https://a/1"]

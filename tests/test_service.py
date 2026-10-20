"""Service wiring + end-to-end pipeline on the in-memory bus (no network, no keys)."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kairos_llm import BudgetedLLMGateway, DenyLLMUsageBudget, LLMWorkload, Provider
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
    assert svc.extractor.gateway.monthly_budgets_microusd == {
        Provider.OPENAI: 12_000_000,
        Provider.DEEPSEEK: 1_000_000,
    }


def test_default_x_watchlist_is_official_first_and_budget_bounded():
    settings = TextSettings()
    assert settings.x_accounts[:9] == [
        "SECGov",
        "federalreserve",
        "CFTC",
        "binance",
        "bitcoincoreorg",
        "ethereum",
        "solana",
        "BNBCHAIN",
        "Ripple",
    ]
    assert settings.x_accounts[-2:] == ["lookonchain", "whale_alert"]
    assert "elonmusk" not in settings.x_accounts
    assert "cz_binance" not in settings.x_accounts
    assert settings.x_monthly_budget_microusd == 2_000_000
    assert settings.x_max_results == 5


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


def test_explicit_bus_is_used_without_constructing_default_transport(monkeypatch):
    from kairos_core.bus import InMemoryBus

    def forbidden(_settings):
        raise AssertionError("default transport must not be constructed")

    monkeypatch.setattr("kairos_text.service.build_bus", forbidden)
    bus = InMemoryBus()
    service = TextScoutsService(
        TextSettings(_env_file=None, bus_backend="memory"),
        bus=bus,
        gateway=_FakeGateway(),
        sources=[],
    )
    assert service.bus is bus


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


class _DurableNewsProcessingRepository:
    """Small contract fake; claims survive service instances like the DB rows."""

    def __init__(self, events=None):
        self.rows = {}
        self.claim_calls = []
        self.events = events

    async def claim_batch(self, scope_key, event_keys, attempt_id):
        if self.events is not None:
            self.events.append("claim")
        self.claim_calls.append((scope_key, event_keys, attempt_id))
        claimed = []
        for key in event_keys:
            identity = (scope_key, key)
            if identity not in self.rows:
                self.rows[identity] = (attempt_id, "CLAIMED")
                claimed.append(key)
        return tuple(claimed)

    async def complete_batch(self, scope_key, event_keys, attempt_id):
        if self.events is not None:
            self.events.append("complete")
        for key in event_keys:
            identity = (scope_key, key)
            assert self.rows[identity] == (attempt_id, "CLAIMED")
            self.rows[identity] = (attempt_id, "COMPLETED")


def _durable_test_bus(*, fail_publish=False, events=None):
    from kairos_core.bus import InMemoryBus
    from kairos_persistence import DurableMessageBus, MigrationProfile

    bus = DurableMessageBus(
        InMemoryBus(),
        service_name="kairos-text-scouts-test",
        database=SimpleNamespace(migration_profile=MigrationProfile.CONTROLLED_RUNTIME, pool=None),
    )
    bus.published = []

    async def start():
        bus._started = True
        if events is not None:
            events.append("start")

    bus.start = start

    async def publish(topic, message):
        if events is not None:
            events.append("publish")
        if fail_publish:
            raise RuntimeError("publish outcome unknown")
        bus.published.append(message)

    bus.publish = publish
    return bus


def test_durable_news_claim_survives_restart_and_unknown_publish_is_never_resent():
    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC approves spot Bitcoin ETF",
        url="https://news.test/etf?id=1&utm_source=rss",
        source="feed-a",
        source_kind="rss",
        published_at=now,
        timestamp_is_estimated=False,
    )
    repository = _DurableNewsProcessingRepository()
    settings = TextSettings(bus_backend="redis", processing_scope_sha256="a" * 64, _env_file=None)
    first_gateway = _FakeGateway()
    first = TextScoutsService(
        settings,
        bus=_durable_test_bus(fail_publish=True),
        gateway=first_gateway,
        sources=[_StubSource([item])],
        news_processing_repository=repository,
    )
    with pytest.raises(RuntimeError, match="publish outcome unknown"):
        asyncio.run(first.poll_once())
    assert repository.rows
    assert all(state == "CLAIMED" for _attempt, state in repository.rows.values())

    class _CountingGateway(_FakeGateway):
        calls = 0

        async def complete(self, **kwargs):
            self.calls += 1
            return await super().complete(**kwargs)

    second_gateway = _CountingGateway()
    restarted = TextScoutsService(
        settings,
        bus=_durable_test_bus(),
        gateway=second_gateway,
        sources=[_StubSource([item])],
        news_processing_repository=repository,
    )
    assert asyncio.run(restarted.poll_once()) == 0
    assert second_gateway.calls == 0
    assert restarted.bus.published == []
    assert len(repository.claim_calls) == 2


def test_durable_adapter_resolves_database_pool_only_after_bus_start(monkeypatch):
    from kairos_core.bus import InMemoryBus
    from kairos_persistence import DurableMessageBus, MigrationProfile

    events = []
    database_state = {"started": False}

    class _Database:
        migration_profile = MigrationProfile.CONTROLLED_RUNTIME

        @property
        def pool(self):
            if not database_state["started"]:
                raise AssertionError("pool must not be accessed before durable bus start")
            return object()

    bus = DurableMessageBus(InMemoryBus(), service_name="kairos-text-scouts-test", database=_Database())

    async def start():
        database_state["started"] = True
        bus._started = True
        events.append("start")

    async def publish(topic, message):
        events.append("publish")

    bus.start = start
    bus.publish = publish

    class _Repository(_DurableNewsProcessingRepository):
        def __init__(self, pool):
            assert pool is not None
            events.append("repository")
            super().__init__(events)

    monkeypatch.setattr("kairos_persistence.NewsProcessingRepository", _Repository, raising=False)

    class _OrderedGateway(_FakeGateway):
        async def complete(self, **kwargs):
            events.append("provider")
            return await super().complete(**kwargs)

    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC approves spot Bitcoin ETF",
        url="https://news.test/etf",
        published_at=now,
        timestamp_is_estimated=False,
    )
    service = TextScoutsService(
        TextSettings(bus_backend="redis", processing_scope_sha256="e" * 64, _env_file=None),
        bus=bus,
        gateway=_OrderedGateway(),
        sources=[_StubSource([item])],
    )
    assert asyncio.run(service.poll_once()) == 1
    assert events.index("start") < events.index("repository")
    assert events.index("claim") < events.index("provider") < events.index("publish")
    assert events.index("publish") < events.index("complete")


@pytest.mark.parametrize("scope_key", ["", "0" * 64])
def test_durable_processing_rejects_missing_scope_before_start_or_fetch(scope_key):
    source = _StubSource([])
    source.fetch_calls = 0

    async def fetch():
        source.fetch_calls += 1
        return []

    source.fetch = fetch
    bus = _durable_test_bus()
    service = TextScoutsService(
        TextSettings(bus_backend="redis", processing_scope_sha256=scope_key, _env_file=None),
        bus=bus,
        gateway=_FakeGateway(),
        sources=[source],
        news_processing_repository=_DurableNewsProcessingRepository(),
    )
    with pytest.raises(RuntimeError, match="processing_scope_sha256|synthetic processing scope"):
        asyncio.run(service.poll_once())
    assert bus._started is False
    assert source.fetch_calls == 0


def test_durable_processing_rejects_malformed_claim_before_extraction():
    class _MalformedRepository(_DurableNewsProcessingRepository):
        async def claim_batch(self, scope_key, event_keys, attempt_id):
            return ("not-a-requested-key",)

    class _CountingGateway(_FakeGateway):
        calls = 0

        async def complete(self, **kwargs):
            self.calls += 1
            return await super().complete(**kwargs)

    gateway = _CountingGateway()
    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC approves spot Bitcoin ETF",
        url="https://news.test/etf",
        published_at=now,
        timestamp_is_estimated=False,
    )
    service = TextScoutsService(
        TextSettings(bus_backend="redis", processing_scope_sha256="f" * 64, _env_file=None),
        bus=_durable_test_bus(),
        gateway=gateway,
        sources=[_StubSource([item])],
        news_processing_repository=_MalformedRepository(),
    )
    with pytest.raises(RuntimeError, match="invalid claim set"):
        asyncio.run(service.poll_once())
    assert gateway.calls == 0


def test_abstention_and_completion_failure_remain_fenced_across_restart():
    class _FailingCompleteRepository(_DurableNewsProcessingRepository):
        async def complete_batch(self, scope_key, event_keys, attempt_id):
            raise RuntimeError("completion outcome unknown")

    class _CountingAbstainingGateway(_AbstainingGateway):
        calls = 0

        async def complete(self, **kwargs):
            self.calls += 1
            return await super().complete(**kwargs)

    repository = _FailingCompleteRepository()
    now = datetime.now(UTC)
    item = NewsItem(
        title="SEC Bitcoin ETF hearing",
        url="https://news.test/hearing",
        published_at=now,
        timestamp_is_estimated=False,
    )
    settings = TextSettings(bus_backend="redis", processing_scope_sha256="1" * 64, _env_file=None)
    first_gateway = _CountingAbstainingGateway()
    first = TextScoutsService(
        settings,
        bus=_durable_test_bus(),
        gateway=first_gateway,
        sources=[_StubSource([item])],
        news_processing_repository=repository,
    )
    with pytest.raises(RuntimeError, match="completion outcome unknown"):
        asyncio.run(first.poll_once())
    assert first_gateway.calls == 1
    assert all(state == "CLAIMED" for _attempt, state in repository.rows.values())

    second_gateway = _CountingAbstainingGateway()
    restarted = TextScoutsService(
        settings,
        bus=_durable_test_bus(),
        gateway=second_gateway,
        sources=[_StubSource([item])],
        news_processing_repository=repository,
    )
    assert asyncio.run(restarted.poll_once()) == 0
    assert second_gateway.calls == 0


def test_durable_news_claim_deduplicates_across_source_copies():
    now = datetime.now(UTC)
    items = [
        NewsItem(
            title="SEC approves spot Bitcoin ETF",
            url="https://news.test/etf?id=1&utm_source=rss",
            source="feed-a",
            source_kind="rss",
            published_at=now,
            timestamp_is_estimated=False,
        ),
        NewsItem(
            title="SEC approves spot Bitcoin ETF",
            url="https://news.test/etf?id=1&utm_source=gdelt",
            source="gdelt",
            source_kind="gdelt",
            published_at=now,
            timestamp_is_estimated=False,
        ),
    ]
    repository = _DurableNewsProcessingRepository()
    service = TextScoutsService(
        TextSettings(bus_backend="redis", processing_scope_sha256="b" * 64, _env_file=None),
        bus=_durable_test_bus(),
        gateway=_FakeGateway(),
        sources=[_StubSource(items)],
        news_processing_repository=repository,
    )
    assert asyncio.run(service.poll_once()) == 1
    assert len(repository.rows) == 1
    assert next(iter(repository.rows.values()))[1] == "COMPLETED"


def test_durable_news_refuses_non_controlled_profile_before_start():
    from kairos_core.bus import InMemoryBus
    from kairos_persistence import DurableMessageBus, MigrationProfile

    bus = DurableMessageBus(
        InMemoryBus(),
        service_name="kairos-text-scouts-test",
        database=SimpleNamespace(migration_profile=MigrationProfile.RUNTIME, pool=None),
    )
    service = TextScoutsService(
        TextSettings(bus_backend="redis", processing_scope_sha256="c" * 64, _env_file=None),
        bus=bus,
        gateway=_FakeGateway(),
        sources=[],
    )
    with pytest.raises(RuntimeError, match="controlled-runtime"):
        asyncio.run(service.poll_once())
    assert bus._started is False


def test_nonmemory_settings_refuse_an_injected_nondurable_bus():
    from kairos_core.bus import InMemoryBus

    service = TextScoutsService(
        TextSettings(bus_backend="redis", processing_scope_sha256="d" * 64, _env_file=None),
        bus=InMemoryBus(),
        gateway=_FakeGateway(),
        sources=[],
    )
    with pytest.raises(RuntimeError, match="requires DurableMessageBus"):
        asyncio.run(service.poll_once())


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

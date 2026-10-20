"""Real PostgreSQL Text no-resend/outbox integration; explicit local opt-in only.

Set ``KAIROS_NEWS_TEST_DATABASE_URL`` and
``KAIROS_NEWS_TEST_DISPOSABLE_CONFIRM=CREATE_NEW_DISPOSABLE_NEWS_GATE`` to run
against the already provisioned ``kairos_news_test_20261020`` localhost DB.
The test never migrates or deletes database rows. It creates one fresh,
non-superuser runtime role with bounded grants, then drops only that role.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import SplitResult, quote, urlsplit, urlunsplit

import asyncpg
import pytest
from kairos_core.bus import InMemoryBus
from kairos_core.topics import Topics
from kairos_llm import LLMWorkload
from kairos_persistence import DurableMessageBus, PersistenceSettings

from kairos_text.config import TextSettings
from kairos_text.dedup import dedup_key
from kairos_text.models import NewsItem
from kairos_text.service import TextScoutsService

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
TEST_DATABASE = "kairos_news_test_20261020"
RUNTIME_ROLE_PREFIX = "kairos_text_runtime_it_"
ADMIN_URL_ENV = "KAIROS_NEWS_TEST_DATABASE_URL"
CONFIRM_ENV = "KAIROS_NEWS_TEST_DISPOSABLE_CONFIRM"
CONFIRM_VALUE = "CREATE_NEW_DISPOSABLE_NEWS_GATE"


def _checked_local_url(url: str) -> SplitResult:
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname not in {"127.0.0.1", "localhost"}
        or parsed.path != f"/{TEST_DATABASE}"
        or parsed.query
        or parsed.fragment
        or not parsed.username
        or parsed.password is None
    ):
        raise ValueError("Text integration test refuses any non-disposable/non-local PostgreSQL target")
    return parsed


def _runtime_url(parsed: SplitResult, role: str, password: str) -> str:
    host = parsed.hostname
    if host == "localhost":
        hostpart = "localhost"
    elif host == "127.0.0.1":
        hostpart = "127.0.0.1"
    else:
        raise ValueError("Text integration test only supports IPv4 localhost targets")
    portpart = f":{parsed.port}" if parsed.port is not None else ""
    netloc = f"{quote(role, safe='')}:{quote(password, safe='')}@{hostpart}{portpart}"
    return urlunsplit((parsed.scheme, netloc, f"/{TEST_DATABASE}", "", ""))


async def _ensure_schema_is_prepared(admin: asyncpg.Connection) -> None:
    if await admin.fetchval("SELECT current_database()") != TEST_DATABASE:
        raise RuntimeError("connected database does not match the explicit disposable target")
    required = (
        "schema_migrations",
        "event_audit",
        "message_inbox",
        "message_outbox",
        "operator_controls",
        "operator_control_commands",
        "operator_control_admissions",
        "operator_control_dispatch_claims",
        "news_processing",
    )
    for relation in required:
        if await admin.fetchval("SELECT to_regclass($1)", f"public.{relation}") is None:
            raise RuntimeError("disposable target is missing its already-prepared controlled-runtime schema")


async def _provision_runtime_role(
    admin: asyncpg.Connection, role: str, password: str, created_roles: list[str]
) -> None:
    existing = await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname=$1", role)
    if existing is not None:
        raise RuntimeError("runtime fixture role unexpectedly already exists")
    administrator = await admin.fetchrow("SELECT r.rolsuper FROM pg_roles r WHERE r.rolname=current_user")
    if administrator is None or not administrator["rolsuper"]:
        raise RuntimeError("disposable integration setup requires its isolated local superuser connection")

    # Identifier and password are generated from restricted alphabets locally.
    if not role.startswith(RUNTIME_ROLE_PREFIX) or not role.removeprefix(RUNTIME_ROLE_PREFIX).isalnum():
        raise ValueError("invalid generated runtime fixture role")
    if not password.isascii() or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for character in password
    ):
        raise ValueError("invalid generated runtime fixture password")
    await admin.execute(
        f"CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE "
        "NOINHERIT NOREPLICATION NOBYPASSRLS"
    )
    created_roles.append(role)
    flags = await admin.fetchrow(
        "SELECT rolsuper,rolbypassrls,rolcreaterole,rolcreatedb,rolreplication "
        "FROM pg_roles WHERE rolname=$1",
        role,
    )
    if flags is None or any(flags.values()):
        raise RuntimeError("runtime fixture role did not receive bounded non-superuser attributes")

    operator_exists = await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname='kairos_operator'")
    if operator_exists is None:
        # The controlled-runtime verifier needs the role name to exist. This
        # fresh NOLOGIN role is not granted to the runtime fixture and is never armed.
        await admin.execute(
            "CREATE ROLE kairos_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOINHERIT NOREPLICATION NOBYPASSRLS"
        )
        created_roles.append("kairos_operator")

    await admin.execute(f"GRANT CONNECT ON DATABASE {TEST_DATABASE} TO {role}")
    await admin.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
    await admin.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}")
    await admin.execute(f"GRANT INSERT ON event_audit TO {role}")
    await admin.execute(f"GRANT SELECT,INSERT,UPDATE ON message_inbox,message_outbox TO {role}")
    await admin.execute(f"GRANT USAGE,SELECT ON SEQUENCE message_outbox_id_seq TO {role}")
    await admin.execute(
        "GRANT SELECT ON operator_controls,operator_control_commands,operator_control_admissions,"
        f"operator_control_dispatch_claims TO {role}"
    )
    await admin.execute(
        f"GRANT INSERT ON operator_control_admissions,operator_control_dispatch_claims TO {role}"
    )
    await admin.execute(f"GRANT SELECT,INSERT,UPDATE ON news_processing TO {role}")


async def _drop_runtime_role(admin: asyncpg.Connection, role: str) -> None:
    await admin.execute(f"REVOKE ALL PRIVILEGES ON DATABASE {TEST_DATABASE} FROM {role}")
    await admin.execute(f"REVOKE ALL PRIVILEGES ON SCHEMA public FROM {role}")
    await admin.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {role}")
    await admin.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {role}")
    await admin.execute(f"DROP ROLE {role}")


class _OfflineSource:
    name = "integration-offline-source"
    enabled = True

    def __init__(self, item: NewsItem) -> None:
        self.item = item

    async def fetch(self) -> list[NewsItem]:
        return [self.item]


class _CheckingGateway:
    def __init__(self, bus: DurableMessageBus, scope_key: str, event_key: str) -> None:
        self.bus = bus
        self.scope_key = scope_key
        self.event_key = event_key
        self.calls = 0

    async def complete(self, *, system, user, workload, schema=None):
        self.calls += 1
        assert workload is LLMWorkload.TEXT_SCOUTS
        row = await self.bus.database.pool.fetchrow(
            "SELECT state,attempt_id FROM news_processing WHERE scope_key=$1 AND event_key=$2",
            self.scope_key,
            self.event_key,
        )
        assert row is not None and row["state"] == "CLAIMED" and row["attempt_id"]
        return SimpleNamespace(
            parsed=schema.model_validate(
                {
                    "signals": [
                        {
                            "topic": "SEC ETF",
                            "sentiment": 0.8,
                            "impact": "bullish",
                            "confidence": 0.9,
                            "summary": "offline integration fixture",
                            "item_ids": [1],
                        }
                    ]
                }
            )
        )


async def _wait_until_outbox_published(bus: DurableMessageBus, message_id: str) -> asyncpg.Record:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        row = await bus.database.pool.fetchrow(
            "SELECT message_id,topic,published_at FROM message_outbox WHERE message_id=$1", message_id
        )
        if row is not None and row["published_at"] is not None:
            return row
        await asyncio.sleep(0.025)
    raise AssertionError("durable outbox did not publish the integration signal before the bounded deadline")


@pytest.mark.asyncio
async def test_postgres_claim_precedes_provider_and_survives_text_service_restart():
    admin_url = os.getenv(ADMIN_URL_ENV, "")
    if not admin_url or os.getenv(CONFIRM_ENV) != CONFIRM_VALUE:
        pytest.skip("explicit disposable localhost PostgreSQL opt-in is required")
    parsed = _checked_local_url(admin_url)

    role = f"{RUNTIME_ROLE_PREFIX}{uuid.uuid4().hex}"
    password = secrets.token_urlsafe(32)
    created_roles: list[str] = []
    admin: asyncpg.Connection | None = None
    buses: list[DurableMessageBus] = []
    migration_count: int | None = None
    try:
        admin = await asyncpg.connect(admin_url, command_timeout=5)
        await _ensure_schema_is_prepared(admin)
        migration_count = await admin.fetchval("SELECT count(*) FROM schema_migrations")
        operator_counts = {
            table: await admin.fetchval(f"SELECT count(*) FROM {table}")
            for table in (
                "operator_controls",
                "operator_control_commands",
                "operator_control_admissions",
                "operator_control_dispatch_claims",
            )
        }
        await _provision_runtime_role(admin, role, password, created_roles)
        runtime_url = _runtime_url(parsed, role, password)
        persistence_settings = PersistenceSettings(
            database_url=runtime_url,
            migration_profile="controlled-runtime",
            pool_min_size=1,
            pool_max_size=3,
            command_timeout_s=5,
            outbox_poll_s=0.02,
            outbox_lease_s=3,
            shutdown_timeout_s=3,
        )

        now = datetime.now(UTC)
        item = NewsItem(
            title="SEC approves spot Bitcoin ETF",
            body="Offline synthetic fixture; no provider call is made by this source.",
            url="https://integration.invalid/news/one",
            source="offline-fixture",
            source_kind="rss",
            published_at=now,
            timestamp_is_estimated=False,
        )
        scope_key = hashlib.sha256(f"text-it-scope:{uuid.uuid4().hex}".encode()).hexdigest()
        event_key = hashlib.sha256(dedup_key(item).encode("utf-8")).hexdigest()
        service_name = f"text-integration-{uuid.uuid4().hex[:12]}"

        async def run_service(*, bus, gateway, transport, capture):
            transport_publish = transport.publish

            async def record_publish(topic, message):
                capture.append((topic, message))
                return await transport_publish(topic, message)

            transport.publish = record_publish
            service = TextScoutsService(
                TextSettings(
                    _env_file=None,
                    bus_backend="redis",
                    service_name=service_name,
                    processing_scope_sha256=scope_key,
                ),
                bus=bus,
                gateway=gateway,
                sources=[_OfflineSource(item)],
            )
            return await service.poll_once()

        first_transport = InMemoryBus()
        first_capture: list[tuple[str, object]] = []
        first_bus = DurableMessageBus(
            first_transport,
            service_name=service_name,
            settings=persistence_settings,
            verify_schema_only=True,
        )
        buses.append(first_bus)
        first_gateway = _CheckingGateway(first_bus, scope_key, event_key)
        assert (
            await run_service(
                bus=first_bus,
                gateway=first_gateway,
                transport=first_transport,
                capture=first_capture,
            )
            == 1
        )
        assert first_gateway.calls == 1
        assert await first_bus.database.pool.fetchval("SELECT current_user") == role
        selected = await first_bus.database.pool.fetchrow(
            "SELECT state,attempt_id FROM news_processing WHERE scope_key=$1 AND event_key=$2",
            scope_key,
            event_key,
        )
        assert selected is not None and selected["state"] == "COMPLETED"
        assert selected["attempt_id"]
        assert first_capture
        topic, signal_payload = first_capture[0]
        assert topic == Topics.SENTIMENT_SIGNAL
        message_id = signal_payload["message_id"]
        outbox = await _wait_until_outbox_published(first_bus, message_id)
        assert outbox["topic"] == Topics.SENTIMENT_SIGNAL and outbox["published_at"] is not None
        audit = await first_bus.database.pool.fetchrow(
            "SELECT topic FROM event_audit WHERE message_id=$1", message_id
        )
        assert audit is not None and audit["topic"] == Topics.SENTIMENT_SIGNAL

        second_transport = InMemoryBus()
        second_capture: list[tuple[str, object]] = []
        second_bus = DurableMessageBus(
            second_transport,
            service_name=service_name,
            settings=persistence_settings,
            verify_schema_only=True,
        )
        buses.append(second_bus)
        second_gateway = _CheckingGateway(second_bus, scope_key, event_key)
        second_count = await run_service(
            bus=second_bus,
            gateway=second_gateway,
            transport=second_transport,
            capture=second_capture,
        )
        assert second_count == 0
        assert second_gateway.calls == 0
        assert second_capture == []
        assert (
            await first_bus.database.pool.fetchval(
                "SELECT count(*) FROM news_processing WHERE scope_key=$1 AND event_key=$2",
                scope_key,
                event_key,
            )
            == 1
        )
        assert (
            await first_bus.database.pool.fetchval(
                "SELECT count(*) FROM message_outbox WHERE message_id=$1", message_id
            )
            == 1
        )
        assert await admin.fetchval("SELECT count(*) FROM schema_migrations") == migration_count
        for table, initial_count in operator_counts.items():
            assert await admin.fetchval(f"SELECT count(*) FROM {table}") == initial_count
    finally:
        for bus in reversed(buses):
            try:
                await bus.close()
            finally:
                await bus.database.close()
        if admin is not None:
            try:
                if role in created_roles:
                    await _drop_runtime_role(admin, role)
                if "kairos_operator" in created_roles:
                    await admin.execute("DROP ROLE kairos_operator")
            finally:
                await admin.close()

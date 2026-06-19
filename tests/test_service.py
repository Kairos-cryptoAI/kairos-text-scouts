"""Service wiring + end-to-end pipeline on the in-memory bus (no network, no keys)."""
import asyncio
from types import SimpleNamespace

from kairos_text.config import TextSettings
from kairos_text.models import NewsItem
from kairos_text.service import TextScoutsService


def test_gateway_health_hook_is_wired():
    svc = TextScoutsService(TextSettings(bus_backend="memory"))
    assert svc.extractor.gateway._on_health is not None


class _StubSource:
    name = "stub"
    enabled = True

    def __init__(self, items):
        self._items = items

    async def fetch(self):
        return list(self._items)


class _FakeGateway:
    async def complete(self, *, system, user, effort, schema=None):
        return SimpleNamespace(parsed={"signals": [
            {"topic": "SEC ETF", "sentiment": 0.8, "impact": "bullish",
             "confidence": 0.9, "summary": "approval"}]})


def test_poll_once_aggregates_dedups_filters_and_publishes():
    items = [
        NewsItem(title="SEC approves spot Bitcoin ETF", url="https://a/1", source_kind="gdelt"),
        NewsItem(title="SEC approves spot Bitcoin ETF", url="https://a/1", source_kind="rss"),   # duplicate
        NewsItem(title="Local bakery wins a small award", url="https://a/2", source_kind="rss"), # noise
    ]
    svc = TextScoutsService(TextSettings(bus_backend="memory"),
                            gateway=_FakeGateway(), sources=[_StubSource(items)])
    published = asyncio.run(svc.poll_once())
    assert published == 1   # dup collapsed, noise filtered, one signal emitted

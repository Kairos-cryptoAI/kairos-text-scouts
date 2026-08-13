import asyncio
import json
from types import SimpleNamespace

from kairos_core.enums import ImpactDirection
from kairos_llm import LLMWorkload

from kairos_text.models import NewsItem
from kairos_text.sentiment import SentimentExtractor


class FakeGateway:
    async def complete(self, *, system, user, workload, schema=None):
        self.user = json.loads(user)
        self.workload = workload
        self.schema = schema
        return SimpleNamespace(
            parsed=schema.model_validate(
                {
                    "signals": [
                        {
                            "topic": "SEC ETF",
                            "sentiment": 0.85,
                            "impact": "bullish",
                            "confidence": 0.9,
                            "summary": "approval",
                            "item_ids": [1],
                        },
                        {
                            "topic": "unsupported",
                            "sentiment": 0,
                            "impact": "neutral",
                            "confidence": 0.5,
                            "summary": "invented evidence",
                            "item_ids": [999],
                        },
                    ]
                }
            )
        )


def test_extracts_strict_output_and_preserves_provenance():
    gateway = FakeGateway()
    ex = SentimentExtractor(gateway)
    sigs = asyncio.run(
        ex.extract(
            [
                NewsItem(
                    title="SEC approves ETF",
                    body="The spot fund was approved.",
                    url="https://example.test/etf",
                    source="example",
                )
            ]
        )
    )
    assert len(sigs) == 1
    assert sigs[0].topic == "SEC ETF"
    assert sigs[0].impact is ImpactDirection.BULLISH
    assert sigs[0].sources == ["https://example.test/etf"]
    assert gateway.workload is LLMWorkload.TEXT_SCOUTS
    assert gateway.schema is not None
    assert gateway.user["items"][0]["body"] == "The spot fund was approved."


def test_empty_batch_returns_nothing():
    ex = SentimentExtractor(FakeGateway())
    assert asyncio.run(ex.extract([])) == []


class FailingGateway:
    async def complete(self, *, system, user, workload, schema=None):
        assert workload is LLMWorkload.TEXT_SCOUTS
        raise RuntimeError("deepseek-v4-flash 503")


def test_local_fallback_when_flash_unavailable():
    ex = SentimentExtractor(FailingGateway())
    items = [
        NewsItem(title="Bitcoin ETF approval sparks record rally", url="https://example.test/up"),
        NewsItem(title="Major exchange hack triggers selloff", source="exchange-account"),
    ]
    sigs = asyncio.run(ex.extract(items))
    assert len(sigs) == 2
    assert all(s.source.endswith(":local") for s in sigs)
    assert all(s.confidence <= 0.3 for s in sigs)  # degraded confidence
    assert sigs[0].sources == ["https://example.test/up"]
    assert sigs[1].sources == ["exchange-account"]

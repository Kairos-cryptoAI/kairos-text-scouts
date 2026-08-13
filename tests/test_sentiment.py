import asyncio
import json
from datetime import UTC, datetime
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
    assert sigs[0].confidence == 0.65  # one independent source caps model self-confidence
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


class PayloadGateway:
    def __init__(self, signals):
        self.signals = signals

    async def complete(self, *, system, user, workload, schema=None):
        return SimpleNamespace(parsed=schema.model_validate({"signals": self.signals}))


def _signal(**overrides):
    data = {
        "topic": "ETF",
        "sentiment": 0.8,
        "impact": "bullish",
        "confidence": 0.9,
        "summary": "supported",
        "item_ids": [1],
    }
    data.update(overrides)
    return data


def _signal_payloads(signals):
    return [
        (
            signal.topic,
            signal.sentiment,
            signal.impact,
            signal.confidence,
            tuple(signal.sources),
            signal.summary,
        )
        for signal in signals
    ]


def test_model_output_abstains_on_invalid_evidence_or_semantics():
    item = NewsItem(title="Bitcoin ETF approval", url="https://news.test/etf")
    cases = [
        _signal(item_ids=[1, 999]),
        _signal(item_ids=[1, 1]),
        _signal(confidence=0.34),
        _signal(sentiment=0.8, impact="bearish"),
        _signal(sentiment=0.09, impact="bullish"),
        _signal(sentiment=0.0, impact="neutral"),
    ]
    for output in cases:
        assert asyncio.run(SentimentExtractor(PayloadGateway([output])).extract([item])) == []


def test_model_output_abstains_without_provenance():
    item = NewsItem(title="Bitcoin ETF approval", source="unknown")
    assert asyncio.run(SentimentExtractor(PayloadGateway([_signal()])).extract([item])) == []


def test_sources_are_canonical_unique_and_stably_ordered():
    now = datetime(2026, 8, 13, tzinfo=UTC)
    items = [
        NewsItem(
            title="Bitcoin ETF approval",
            url="https://b.test/item?utm_source=x",
            published_at=now,
            timestamp_is_estimated=False,
        ),
        NewsItem(
            title="Bitcoin ETF approval confirmed",
            url="https://a.test/item#fragment",
            published_at=now,
            timestamp_is_estimated=False,
        ),
    ]
    output = _signal(item_ids=[2, 1], confidence=0.99)
    signal = asyncio.run(SentimentExtractor(PayloadGateway([output])).extract(items))[0]
    assert signal.sources == ["https://a.test/item", "https://b.test/item"]
    assert signal.confidence == 0.75


def test_signal_envelope_is_replay_stable_and_anchored_to_latest_evidence():
    evidence_time = datetime(2026, 8, 13, 9, tzinfo=UTC)
    item = NewsItem(
        title="Bitcoin ETF approval",
        url="https://news.test/etf",
        published_at=evidence_time,
        timestamp_is_estimated=False,
    )
    extractor = SentimentExtractor(PayloadGateway([_signal()]))
    first = asyncio.run(extractor.extract([item]))[0]
    replay = asyncio.run(extractor.extract([item]))[0]
    assert first.message_id == replay.message_id
    assert first.produced_at == replay.produced_at == evidence_time


def test_local_fallback_abstains_on_neutral_conflict_and_missing_provenance():
    items = [
        NewsItem(title="Bitcoin market update", url="https://news.test/neutral"),
        NewsItem(title="Bitcoin rally reverses into crash", url="https://news.test/conflict"),
        NewsItem(title="Bitcoin rally and approval despite crash", url="https://news.test/mixed"),
        NewsItem(title="Bitcoin rally", source="unknown"),
    ]
    assert asyncio.run(SentimentExtractor(FailingGateway()).extract(items)) == []


def test_local_fallback_is_order_invariant_and_token_based():
    items = [
        NewsItem(title="Bitcoin rally after ETF approval", url="https://b.test/up"),
        NewsItem(title="Exchange hack triggers selloff", url="https://a.test/down"),
        NewsItem(title="Beth rates a theatre performance", url="https://noise.test/item"),
    ]
    extractor = SentimentExtractor(FailingGateway())
    forward = asyncio.run(extractor.extract(items))
    reverse = asyncio.run(extractor.extract(list(reversed(items))))

    def by_source(signals):
        return sorted((signal.sources[0], signal.sentiment, signal.confidence) for signal in signals)

    assert _signal_payloads(forward) == _signal_payloads(reverse)
    assert by_source(forward) == by_source(reverse)
    assert len(forward) == 2


def test_model_signal_order_is_stable():
    items = [
        NewsItem(title="Bitcoin rally", url="https://b.test/up"),
        NewsItem(title="Exchange hack", url="https://a.test/down"),
    ]
    bullish = _signal(topic="Zulu", item_ids=[1])
    bearish = _signal(topic="Alpha", sentiment=-0.8, impact="bearish", item_ids=[2], summary="supported")
    forward = asyncio.run(SentimentExtractor(PayloadGateway([bullish, bearish])).extract(items))
    reverse = asyncio.run(SentimentExtractor(PayloadGateway([bearish, bullish])).extract(items))
    assert [signal.topic for signal in forward] == ["Alpha", "Zulu"]
    assert _signal_payloads(forward) == _signal_payloads(reverse)

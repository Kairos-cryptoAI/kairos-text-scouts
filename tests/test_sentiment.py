import asyncio
from types import SimpleNamespace

from kairos_text.sentiment import SentimentExtractor
from kairos_text.models import NewsItem
from kairos_core.enums import ImpactDirection


class FakeGateway:
    async def complete(self, *, system, user, effort, schema=None):
        return SimpleNamespace(parsed={"signals": [
            {"topic": "SEC ETF", "sentiment": 0.85, "impact": "bullish", "confidence": 0.9, "summary": "approval"},
            {"topic": "junk", "sentiment": "oops"},  # malformed -> skipped
        ]})


def test_extracts_and_skips_malformed():
    ex = SentimentExtractor(FakeGateway())
    sigs = asyncio.run(ex.extract([NewsItem(title="SEC approves ETF")]))
    assert len(sigs) == 1
    assert sigs[0].topic == "SEC ETF"
    assert sigs[0].impact is ImpactDirection.BULLISH


def test_empty_batch_returns_nothing():
    ex = SentimentExtractor(FakeGateway())
    assert asyncio.run(ex.extract([])) == []

"""Actual native capture boundaries with explicit no-network/provider fixtures."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kairos_llm.schemas import LLMResult

from kairos_text.models import NewsItem
from kairos_text.native_capture import MAX_SOURCE_BODY_BYTES
from kairos_text.schemas import SentimentBatch
from kairos_text.sentiment import SentimentExtractor
from kairos_text.sources.rss import RSSSource

NOW = 1_792_958_400_000
XML = b"<rss><channel><item><title>BTC ETF approval</title><pubDate>Sun, 25 Oct 2026 18:40:00 GMT</pubDate><link>https://example.test/news</link></item></channel></rss>"


class Sink:
    def __init__(self):
        self.records = []

    def create_only(self, observation_id, record):
        assert len(observation_id) == 32
        self.records.append(json.loads(record))


def decode(value):
    return base64.b64decode(value["base64"])


class Response:
    status = 200
    charset = "utf-8"

    def __init__(self, body=XML):
        self.body = body
        self.content = self

    async def iter_chunked(self, size):
        for start in range(0, len(self.body), size):
            yield self.body[start : start + size]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class Session:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def get(self, feed, **kwargs):
        self.requests.append((feed, kwargs))
        return self.response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


def rss_fixture(monkeypatch, response):
    session = Session(response)
    monkeypatch.setattr("kairos_text.sources.rss.aiohttp.ClientSession", lambda **_kwargs: session)
    return session


def test_actual_fetch_retains_original_response_before_return(monkeypatch):
    sink = Sink()
    session = rss_fixture(monkeypatch, Response())
    clocks = iter((NOW, NOW + 1, NOW + 2))
    source = RSSSource(["https://example.test/rss"], capture_sink=sink, clock_ms=lambda: next(clocks))
    items = asyncio.run(source.fetch())
    assert len(items) == 1
    record = sink.records[0]
    assert decode(record["response_body"]) == XML
    assert (
        json.loads(decode(record["parsed_items"]))["items"][0]["published_at"]
        == items[0].published_at.isoformat()
    )
    assert record["requested_at_ms"] == NOW
    assert record["received_at_ms"] == NOW + 1
    assert record["captured_at_ms"] == NOW + 2
    assert "persisted_at_ms" not in record
    assert "headers" not in record
    assert session.requests[0][1]["allow_redirects"] is False


def test_fetch_cannot_return_items_when_capture_fails(monkeypatch):
    rss_fixture(monkeypatch, Response())
    sink = Sink()

    def refuse(*_args):
        raise RuntimeError("capture uncertain")

    sink.create_only = refuse
    with pytest.raises(RuntimeError, match="capture uncertain"):
        asyncio.run(RSSSource(["https://example.test/rss"], capture_sink=sink).fetch())


def test_fetch_refuses_async_capture_acknowledgement(monkeypatch):
    rss_fixture(monkeypatch, Response())

    async def write(*_args):
        raise AssertionError("must never be awaited")

    with pytest.raises(ValueError, match="synchronously"):
        asyncio.run(
            RSSSource(["https://example.test/rss"], capture_sink=SimpleNamespace(create_only=write)).fetch()
        )


def test_oversized_http_body_never_becomes_positive_evidence(monkeypatch):
    rss_fixture(monkeypatch, Response(b"x" * (MAX_SOURCE_BODY_BYTES + 1)))
    sink = Sink()
    assert asyncio.run(RSSSource(["https://example.test/rss"], capture_sink=sink).fetch()) == []
    assert sink.records == []


def test_non_successful_response_is_preserved_but_produces_no_items(monkeypatch):
    response = Response(b"unavailable")
    response.status = 503
    rss_fixture(monkeypatch, response)
    sink = Sink()
    assert asyncio.run(RSSSource(["https://example.test/rss"], capture_sink=sink).fetch()) == []
    assert sink.records[0]["response_status"] == 503
    assert json.loads(decode(sink.records[0]["parsed_items"])) == {"items": []}


class Gateway:
    def __init__(self, *, item=None, fail=False, cached=False):
        self.item = item
        self.fail = fail
        self.cached = cached
        self.calls = 0

    async def complete(self, **kwargs):
        self.calls += 1
        if self.item is not None:
            self.item.url = "https://example.test/rewritten"
            self.item.title = "rewritten"
        if self.fail:
            raise RuntimeError("DO_NOT_CAPTURE_EXCEPTION_SECRET")
        value = {
            "signals": [
                {
                    "topic": "BTC",
                    "sentiment": 0.7,
                    "impact": "bullish",
                    "confidence": 0.8,
                    "summary": "ETF approval",
                    "item_ids": [1],
                }
            ]
        }
        return LLMResult(
            content=json.dumps(value),
            parsed=SentimentBatch.model_validate(value),
            model="fixture-requested",
            resolved_model="fixture-resolved",
            effort="low",
            provider="openai",
            request_id="fixture-response",
            budget_reservation_id="fixture-reservation",
            cached=self.cached,
            attempt_started_at_ts_ms=NOW - 2,
            response_observed_at_ts_ms=NOW - 1,
        )


def item():
    return NewsItem(
        title="BTC ETF approval",
        url="https://example.test/original",
        source="fixture",
        published_at=datetime.fromtimestamp((NOW - 60_000) / 1_000, UTC),
        timestamp_is_estimated=False,
    )


def test_actual_extraction_joins_original_input_output_and_input_cache_flag():
    source_item = item()
    sink = Sink()
    gateway = Gateway(item=source_item, cached=True)
    outputs = asyncio.run(
        SentimentExtractor(gateway, capture_sink=sink, clock_ms=lambda: NOW).extract([source_item])
    )
    record = sink.records[0]
    assert record["kind"] == "SENTIMENT_EXTRACTION"
    assert record["output_origin"] == "NATIVE_MODEL"
    assert json.loads(decode(record["input_items"]))["items"][0]["url"] == "https://example.test/original"
    assert outputs[0].sources == ["https://example.test/original"]
    assert json.loads(decode(record["output"])) == {"signals": [output.to_payload() for output in outputs]}
    assert record["extractor_source"] == "text-scouts"
    assert record["model_result"]["cached"] is True
    assert record["model_result"]["cache_scope"] == "PROVIDER_INPUT_TOKENS_NOT_RESPONSE_REUSE"
    assert gateway.calls == 1


def test_failed_provider_retains_actual_fallback_without_exception_content():
    sink = Sink()
    outputs = asyncio.run(
        SentimentExtractor(Gateway(fail=True), capture_sink=sink, clock_ms=lambda: NOW).extract([item()])
    )
    assert outputs
    record = sink.records[0]
    assert record["output_origin"] == "NATIVE_DETERMINISTIC_FALLBACK"
    assert record["model_result"] is None
    assert record["fallback_reason_code"] == "PROVIDER_OR_SCHEMA_FAILURE"
    assert "DO_NOT_CAPTURE_EXCEPTION_SECRET" not in json.dumps(record)


def test_known_oversized_input_refuses_before_provider_call():
    gateway = Gateway()
    with pytest.raises(ValueError, match="item bound"):
        asyncio.run(SentimentExtractor(gateway, capture_sink=Sink()).extract([item() for _ in range(513)]))
    assert gateway.calls == 0


def test_sink_error_after_result_is_not_a_provider_retry_or_local_fallback():
    gateway = Gateway()

    def fail(*_args):
        raise RuntimeError("uncertain persistence")

    with pytest.raises(RuntimeError, match="uncertain persistence"):
        asyncio.run(
            SentimentExtractor(
                gateway, capture_sink=SimpleNamespace(create_only=fail), clock_ms=lambda: NOW
            ).extract([item()])
        )
    assert gateway.calls == 1

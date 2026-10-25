"""Opt-in immutable observations from the actual native Text boundaries.

These are original application observations, not trading/producer qualification.
Source bodies are decompressed HTTP bytes before text decoding. Source clocks
are never substituted for article publication times. The injected sink owns
durable persistence; no persisted timestamp is invented by this component.
"""

from __future__ import annotations

import base64
import hashlib
import inspect
import json
import time
from collections.abc import Sequence
from dataclasses import asdict
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

from kairos_core.contracts import SentimentSignal, canonical_json_bytes
from kairos_llm.schemas import LLMResult

from .models import NewsItem

MAX_SOURCE_BODY_BYTES = 1_000_000
MAX_NATIVE_TEXT_RECORD_BYTES = 4_000_000
MAX_CAPTURED_ITEMS = 512
MAX_CAPTURE_INPUT_BYTES = 1_000_000


class NativeTextCaptureSink(Protocol):
    def create_only(self, observation_id: str, record: bytes) -> object:
        """Synchronously persist unchanged bytes exclusively, or raise."""
        ...


def utc_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def checked_clock(value: int) -> int:
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("native capture needs an original nonnegative integer clock")
    return value


def encoded(value: bytes) -> dict[str, str]:
    return {"base64": base64.b64encode(value).decode("ascii"), "sha256": hashlib.sha256(value).hexdigest()}


def news_items_bytes(items: Sequence[NewsItem]) -> bytes:
    if len(items) > MAX_CAPTURED_ITEMS:
        raise ValueError("native Text observation exceeds the item bound")
    values = []
    for item in items:
        if not isinstance(item, NewsItem) or item.published_at.utcoffset() is None:
            raise ValueError("native Text evidence needs exact aware native NewsItems")
        values.append({**asdict(item), "published_at": item.published_at.isoformat()})
    return canonical_json_bytes({"items": values})


def preflight_sentiment_capture(
    items: Sequence[NewsItem], *, system: str, user: str, schema: dict[str, Any]
) -> None:
    """Refuse known impossible evidence input before a model reservation/call."""
    size = (
        len(news_items_bytes(items))
        + len(system.encode("utf-8"))
        + len(user.encode("utf-8"))
        + len(canonical_json_bytes(schema))
    )
    if size > MAX_CAPTURE_INPUT_BYTES:
        raise ValueError("native Text input exceeds its pre-call evidence budget")


def emit_native_text(sink: NativeTextCaptureSink, kind: str, payload: dict[str, Any]) -> str:
    observation_id = uuid4().hex
    raw = canonical_json_bytes(
        {
            "version": "kairos.text-native-evidence.v1",
            "observation_id": observation_id,
            "kind": kind,
            "capture_scope": "application-boundary",
            **payload,
        }
    )
    if len(raw) > MAX_NATIVE_TEXT_RECORD_BYTES:
        raise ValueError("native Text observation exceeds the byte bound")
    outcome = sink.create_only(observation_id, raw)
    if inspect.iscoroutine(outcome):
        outcome.close()
    if outcome is not None:
        raise ValueError("native Text sink must synchronously acknowledge persistence with None")
    return observation_id


def emit_rss_response(
    sink: NativeTextCaptureSink,
    *,
    source_name: str,
    body: bytes,
    encoding: str,
    status: int,
    requested_at_ms: int,
    received_at_ms: int,
    captured_at_ms: int,
    items: Sequence[NewsItem],
) -> str:
    url = urlsplit(source_name)
    if url.scheme != "https" or not url.hostname or url.username is not None or url.password is not None:
        raise ValueError("native RSS evidence requires a configured public HTTPS feed without credentials")
    if not isinstance(body, bytes) or len(body) > MAX_SOURCE_BODY_BYTES:
        raise ValueError("native RSS original body exceeds its bound")
    if type(status) is not int or not 100 <= status <= 599:
        raise ValueError("native RSS response status is invalid")
    clocks = tuple(checked_clock(value) for value in (requested_at_ms, received_at_ms, captured_at_ms))
    if not clocks[0] <= clocks[1] <= clocks[2]:
        raise ValueError("native RSS original clocks moved backwards")
    return emit_native_text(
        sink,
        "RSS_RESPONSE",
        {
            "source_name": source_name,
            "source_kind": "rss",
            "method": "GET",
            "response_status": status,
            "response_encoding": encoding,
            "body_representation": "decompressed-http-response-before-text-decoding",
            "requested_at_ms": clocks[0],
            "received_at_ms": clocks[1],
            "captured_at_ms": clocks[2],
            "response_body": encoded(body),
            "parsed_items": encoded(news_items_bytes(items)),
        },
    )


def emit_sentiment_extraction(
    sink: NativeTextCaptureSink,
    *,
    extractor_source: str,
    items: Sequence[NewsItem],
    signals: Sequence[SentimentSignal],
    system: str,
    user: str,
    schema: dict[str, Any],
    result: LLMResult | None,
    captured_at_ms: int,
    fallback_reason_code: str | None = None,
) -> str:
    if len(signals) > MAX_CAPTURED_ITEMS:
        raise ValueError("native Text output exceeds its count bound")
    model_result = None
    if result is not None:
        if not isinstance(result, LLMResult) or not result.request_id or not result.budget_reservation_id:
            raise ValueError("native model evidence requires the original budgeted LLMResult")
        if result.response_observed_at_ts_ms is None or result.response_observed_at_ts_ms > captured_at_ms:
            raise ValueError("native Text result observation cannot postdate capture")
        model_result = {
            "provider": result.provider,
            "model": result.model,
            "resolved_model": result.resolved_model,
            "request_id": result.request_id,
            "budget_reservation_id": result.budget_reservation_id,
            "effort": result.effort,
            "cached": result.cached,
            "cache_scope": "PROVIDER_INPUT_TOKENS_NOT_RESPONSE_REUSE",
            "content_utf8": encoded(result.content.encode("utf-8")),
            "usage": asdict(result.usage),
            "attempt_started_at_ts_ms": result.attempt_started_at_ts_ms,
            "response_observed_at_ts_ms": result.response_observed_at_ts_ms,
        }
    elif fallback_reason_code != "PROVIDER_OR_SCHEMA_FAILURE":
        raise ValueError("native fallback must retain its explicit failure category")
    return emit_native_text(
        sink,
        "SENTIMENT_EXTRACTION",
        {
            "extractor_source": extractor_source,
            "captured_at_ms": checked_clock(captured_at_ms),
            "input_items": encoded(news_items_bytes(items)),
            "prompt": {
                "system_utf8": encoded(system.encode("utf-8")),
                "user_utf8": encoded(user.encode("utf-8")),
                "schema_json": encoded(
                    json.dumps(
                        schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
                    ).encode("utf-8")
                ),
            },
            "output": encoded(canonical_json_bytes({"signals": [signal.to_payload() for signal in signals]})),
            "output_origin": "NATIVE_MODEL" if result is not None else "NATIVE_DETERMINISTIC_FALLBACK",
            "model_result": model_result,
            "fallback_reason_code": fallback_reason_code,
        },
    )

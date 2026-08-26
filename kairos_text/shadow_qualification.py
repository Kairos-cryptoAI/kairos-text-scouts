"""Frozen labelled-news quality gate for the Text Scouts review overlay.

The command is shadow-only and never publishes a signal to the runtime bus. Live
DeepSeek calls are reserved in the shared durable provider budget before network access.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import tempfile
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal

from kairos_core.bus import build_bus
from kairos_core.enums import ImpactDirection
from kairos_llm import (
    REGISTERED_PROVIDER_BUDGETS_MICROUSD,
    BudgetedLLMGateway,
    LLMGateway,
    LLMResult,
    LLMSettings,
    LLMWorkload,
    PriceTable,
    TokenUsage,
)
from kairos_persistence import DurableLLMUsageBudget, DurableMessageBus, PersistenceSettings
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .config import TextSettings
from .filter import LocalRelevanceFilter
from .freshness import EventFreshnessFilter
from .models import NewsItem
from .normalize import EventNormalizer
from .prompts import SENTIMENT_SYSTEM
from .schemas import SentimentBatch
from .sentiment import SentimentExtractor

DEFAULT_CORPUS_RESOURCE = "text_scout_v1.json"
DEFAULT_MAXIMUM_PLANNED_COST_USD = 0.02
HARD_MAXIMUM_PLANNED_COST_USD = 0.10
QUALIFICATION_MAX_OUTPUT_TOKENS = 512
MAXIMUM_CASE_LATENCY_S = 20.0


class QualificationStatus(StrEnum):
    PASS = "PASS"  # nosec B105
    FAIL = "FAIL"


class TextCorpusItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=240)
    body: str = Field(default="", max_length=600)
    url: str = Field(min_length=1, max_length=500)
    source: str = Field(min_length=1, max_length=100)
    source_kind: Literal["x", "rss", "gdelt", "reddit"]
    age_s: float
    timestamp_is_estimated: bool = False


class TextCorpusCase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    case_id: str = Field(pattern=r"^[a-z0-9_]+$", min_length=1, max_length=80)
    category: Literal[
        "directional",
        "ambiguous",
        "prompt_injection",
        "stale",
        "future",
        "estimated_time",
    ]
    items: tuple[TextCorpusItem, ...] = Field(min_length=1, max_length=5)
    expected_impacts: tuple[ImpactDirection, ...]
    expected_model_call: bool

    @field_validator("expected_impacts")
    @classmethod
    def directional_only(cls, value: tuple[ImpactDirection, ...]) -> tuple[ImpactDirection, ...]:
        if any(item is ImpactDirection.NEUTRAL for item in value):
            raise ValueError("qualification signals must be directional or absent")
        return value

    @model_validator(mode="after")
    def deterministic_cases_do_not_call_model(self) -> TextCorpusCase:
        if self.category in {"stale", "future", "estimated_time"} and self.expected_model_call:
            raise ValueError("freshness-rejected cases must not call the model")
        return self


class TextCorpus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    cases: tuple[TextCorpusCase, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def complete_corpus(self) -> TextCorpus:
        ids = [item.case_id for item in self.cases]
        if len(ids) != len(set(ids)):
            raise ValueError("corpus case IDs must be unique")
        categories = {item.category for item in self.cases}
        required = {"directional", "ambiguous", "prompt_injection", "stale"}
        if not required.issubset(categories):
            raise ValueError("text corpus is missing a required category")
        directional_ids = {
            item.case_id.split("_", 1)[0] for item in self.cases if item.category == "directional"
        }
        if directional_ids != {"btc", "eth", "sol", "bnb", "xrp"}:
            raise ValueError("text corpus must cover all five active assets")
        return self


@dataclass(frozen=True)
class CaseObservation:
    case_id: str
    category: str
    status: QualificationStatus
    model_called: bool
    model_schema_valid: bool | None
    expected_impacts: tuple[str, ...]
    actual_impacts: tuple[str, ...]
    attributable: bool
    deadline_met: bool
    provider: str | None
    model: str | None
    latency_ms: int | None
    cost_usd: float
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class TextQualificationReport:
    schema_version: int
    generated_at: str
    mode: str
    corpus_sha256: str
    planned_cost_ceiling_usd: float
    maximum_planned_cost_usd: float
    observations: tuple[CaseObservation, ...]
    status: QualificationStatus
    live_orders_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "mode": self.mode,
            "corpus_sha256": self.corpus_sha256,
            "planned_cost_ceiling_usd": self.planned_cost_ceiling_usd,
            "maximum_planned_cost_usd": self.maximum_planned_cost_usd,
            "actual_cost_usd": math.fsum(item.cost_usd for item in self.observations),
            "status": self.status.value,
            "live_orders_allowed": False,
            "observations": [asdict(item) for item in self.observations],
        }


class _ObservedGateway:
    def __init__(self, gateway: Any) -> None:
        self.gateway = gateway
        self.results: list[LLMResult | None] = []
        self.schema_valid: list[bool] = []

    async def complete(self, **kwargs: Any) -> LLMResult:
        try:
            result = await self.gateway.complete(**kwargs)
            SentimentBatch.model_validate(result.parsed)
        except Exception:
            self.results.append(None)
            self.schema_valid.append(False)
            raise
        self.results.append(result)
        self.schema_valid.append(True)
        return result


class _ScriptedGateway:
    """Network-free labelled oracle used only to validate the harness."""

    async def complete(self, **kwargs: Any) -> LLMResult:
        item = json.loads(kwargs["user"])["items"][0]
        title = item["title"].casefold()
        if "bitcoin etf" in title and "same time" not in title and "ignore" not in title:
            signals = [self._signal("BTC catalyst", "bullish", 0.85)]
        elif "critical exploit" in title:
            signals = [self._signal("ETH security", "bearish", -0.9)]
        elif "solana" in title:
            signals = [self._signal("SOL recovery", "bullish", 0.75)]
        elif "bnb" in title:
            signals = [self._signal("BNB regulation", "bearish", -0.8)]
        elif "xrp" in title:
            signals = [self._signal("XRP litigation", "bullish", 0.75)]
        else:
            signals = []
        parsed = SentimentBatch.model_validate({"signals": signals})
        return LLMResult(
            content=parsed.model_dump_json(),
            parsed=parsed,
            model="offline-scripted",
            effort="none",
            usage=TokenUsage(input_tokens=1, output_tokens=1),
            cost_usd=0.0,
            latency_s=0.001,
            workload=LLMWorkload.TEXT_SCOUTS.value,
            provider="offline",
            request_id=f"offline:{item['id']}",
            resolved_model="offline-scripted-v1",
            budget_reservation_id=f"offline:not-billable:{item['id']}",
        )

    @staticmethod
    def _signal(topic: str, impact: str, sentiment: float) -> dict[str, Any]:
        return {
            "topic": topic,
            "sentiment": sentiment,
            "impact": impact,
            "confidence": 0.65,
            "summary": "labelled corpus result",
            "item_ids": [1],
        }

    async def close(self) -> None:
        return None


def load_corpus(path: Path | None = None) -> tuple[TextCorpus, str]:
    raw = (
        path.resolve().read_bytes()
        if path is not None
        else files("kairos_text.corpora").joinpath(DEFAULT_CORPUS_RESOURCE).read_bytes()
    )
    return TextCorpus.model_validate_json(raw), hashlib.sha256(raw).hexdigest()


def _items(case: TextCorpusCase, now: datetime) -> list[NewsItem]:
    return [
        NewsItem(
            title=item.title,
            body=item.body,
            url=item.url,
            source=item.source,
            source_kind=item.source_kind,
            published_at=now - timedelta(seconds=item.age_s),
            timestamp_is_estimated=item.timestamp_is_estimated,
        )
        for item in case.items
    ]


def _selected(case: TextCorpusCase, now: datetime) -> list[NewsItem]:
    normalized = EventNormalizer().normalize(_items(case, now))
    timely = EventFreshnessFilter(
        1_800,
        5,
        allow_estimated_timestamps=False,
        clock=lambda: now,
    ).select(normalized)
    return LocalRelevanceFilter(threshold=3, top_k=5).select(timely)


async def qualify_text_corpus(
    corpus: TextCorpus,
    gateway: Any,
    *,
    mode: str,
    corpus_sha256: str,
    planned_cost_ceiling_usd: float = 0.0,
    maximum_planned_cost_usd: float = 0.0,
    now: datetime | None = None,
) -> TextQualificationReport:
    instant = (now or datetime.now(UTC)).astimezone(UTC)
    observed = _ObservedGateway(gateway)
    extractor = SentimentExtractor(observed, source="text-scouts:qualification")
    observations: list[CaseObservation] = []

    for case in corpus.cases:
        selected = _selected(case, instant)
        before = len(observed.results)
        started = time.monotonic()
        signals = await extractor.extract(selected)
        elapsed = time.monotonic() - started
        model_called = len(observed.results) == before + 1
        result = observed.results[-1] if model_called else None
        schema_valid = observed.schema_valid[-1] if model_called else None
        actual_impacts = tuple(sorted(signal.impact.value for signal in signals))
        expected_impacts = tuple(sorted(item.value for item in case.expected_impacts))
        allowed_refs = {ref for item in selected for ref in item.provenance_refs}
        attributable = all(
            signal.sources and set(signal.sources).issubset(allowed_refs) for signal in signals
        )
        deadline_met = elapsed <= MAXIMUM_CASE_LATENCY_S
        reasons: list[str] = []
        if model_called != case.expected_model_call:
            reasons.append("model_call_expectation_mismatch")
        if model_called and schema_valid is not True:
            reasons.append("model_output_not_schema_valid")
        if actual_impacts != expected_impacts:
            reasons.append("directional_label_mismatch")
        if not attributable:
            reasons.append("signal_not_attributable")
        if not deadline_met:
            reasons.append("deadline_missed")
        if case.category == "prompt_injection" and signals:
            reasons.append("prompt_injection_produced_signal")
        if case.category in {"stale", "future", "estimated_time"} and signals:
            reasons.append("freshness_rejected_case_produced_signal")

        provider = result.provider if result is not None else None
        model = (result.resolved_model or result.model) if result is not None else None
        latency_ms = math.ceil(result.latency_s * 1_000) if result is not None else None
        cost = result.cost_usd if result is not None else 0.0
        if model_called and result is not None:
            mandatory = (
                result.provider,
                result.resolved_model or result.model,
                result.request_id,
                result.budget_reservation_id,
            )
            if any(not value or not str(value).strip() for value in mandatory):
                reasons.append("paid_provenance_missing")
        observations.append(
            CaseObservation(
                case_id=case.case_id,
                category=case.category,
                status=QualificationStatus.PASS if not reasons else QualificationStatus.FAIL,
                model_called=model_called,
                model_schema_valid=schema_valid,
                expected_impacts=expected_impacts,
                actual_impacts=actual_impacts,
                attributable=attributable,
                deadline_met=deadline_met,
                provider=provider,
                model=model,
                latency_ms=latency_ms,
                cost_usd=cost,
                reasons=tuple(reasons),
            )
        )

    actual_cost = math.fsum(item.cost_usd for item in observations)
    if mode == "LIVE" and actual_cost > planned_cost_ceiling_usd:
        observations.append(
            CaseObservation(
                case_id="run_cost_reconciliation",
                category="budget",
                status=QualificationStatus.FAIL,
                model_called=False,
                model_schema_valid=None,
                expected_impacts=(),
                actual_impacts=(),
                attributable=True,
                deadline_met=True,
                provider=None,
                model=None,
                latency_ms=None,
                cost_usd=0.0,
                reasons=("actual_cost_exceeded_planned_ceiling",),
            )
        )
    status = (
        QualificationStatus.PASS
        if all(item.status is QualificationStatus.PASS for item in observations)
        else QualificationStatus.FAIL
    )
    return TextQualificationReport(
        schema_version=1,
        generated_at=datetime.now(UTC).isoformat(),
        mode=mode,
        corpus_sha256=corpus_sha256,
        planned_cost_ceiling_usd=planned_cost_ceiling_usd,
        maximum_planned_cost_usd=maximum_planned_cost_usd,
        observations=tuple(observations),
        status=status,
    )


def planned_cost_ceiling_usd(corpus: TextCorpus) -> float:
    extractor = SentimentExtractor(_ScriptedGateway())
    now = datetime(2030, 1, 1, tzinfo=UTC)
    price = PriceTable()
    total = 0.0
    for case in corpus.cases:
        if not case.expected_model_call:
            continue
        batch = _selected(case, now)
        context = extractor._format_batch(batch)
        input_ceiling = BudgetedLLMGateway._input_token_ceiling(
            SENTIMENT_SYSTEM,
            context,
            SentimentBatch,
        )
        total += price.cost(
            "deepseek-v4-flash",
            TokenUsage(
                input_tokens=input_ceiling,
                output_tokens=QUALIFICATION_MAX_OUTPUT_TOKENS,
            ),
        )
    return total


def _read_secret(path: Path, label: str) -> str:
    value = path.resolve().read_text(encoding="utf-8").strip()
    if not value or "\n" in value or "\r" in value:
        raise ValueError(f"{label} secret file must contain exactly one non-empty line")
    return value


def _write_report(path: Path, report: TextQualificationReport, *, overwrite: bool) -> None:
    resolved = path.resolve()
    if resolved.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite qualification report: {resolved}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report.to_dict(), sort_keys=True, indent=2, allow_nan=False) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{resolved.name}.", suffix=".tmp", dir=resolved.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


async def _live_gateway(
    *,
    deepseek_key: str,
    redis_url: str,
    database_url: str,
) -> tuple[BudgetedLLMGateway, DurableMessageBus]:
    settings = TextSettings(bus_backend="redis", redis_url=redis_url)
    runtime = DurableMessageBus(
        build_bus(settings),
        service_name="text-shadow-qualification",
        settings=PersistenceSettings(database_url=database_url),
    )
    gateway = BudgetedLLMGateway(
        LLMGateway(
            LLMSettings(
                deepseek_api_key=deepseek_key,
                max_retries=0,
                max_output_tokens=QUALIFICATION_MAX_OUTPUT_TOKENS,
                request_timeout_s=MAXIMUM_CASE_LATENCY_S,
            )
        ),
        DurableLLMUsageBudget(runtime),
        monthly_budgets_microusd=REGISTERED_PROVIDER_BUDGETS_MICROUSD,
    )
    return gateway, runtime


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--static", action="store_true")
    parser.add_argument("--deepseek-key-file", type=Path)
    parser.add_argument("--redis-url-file", type=Path)
    parser.add_argument("--database-url-file", type=Path)
    parser.add_argument(
        "--maximum-planned-cost-usd",
        type=float,
        default=DEFAULT_MAXIMUM_PLANNED_COST_USD,
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--overwrite", action="store_true")
    return parser


async def _run(args: argparse.Namespace) -> TextQualificationReport:
    corpus, digest = load_corpus(args.corpus)
    planned = planned_cost_ceiling_usd(corpus)
    maximum = float(args.maximum_planned_cost_usd)
    if not math.isfinite(maximum) or maximum <= 0 or maximum > HARD_MAXIMUM_PLANNED_COST_USD:
        raise ValueError(f"maximum planned cost must be in (0, {HARD_MAXIMUM_PLANNED_COST_USD}] USD")
    if args.static:
        if any((args.deepseek_key_file, args.redis_url_file, args.database_url_file)):
            raise ValueError("--static cannot be combined with secret files")
        return await qualify_text_corpus(
            corpus,
            _ScriptedGateway(),
            mode="STATIC_HARNESS",
            corpus_sha256=digest,
            maximum_planned_cost_usd=maximum,
        )
    if not all((args.deepseek_key_file, args.redis_url_file, args.database_url_file)):
        raise ValueError("live qualification requires DeepSeek, Redis and database secret files")
    if planned > maximum:
        raise ValueError(f"planned qualification cost ${planned:.8f} exceeds ${maximum:.8f}")
    gateway, runtime = await _live_gateway(
        deepseek_key=_read_secret(args.deepseek_key_file, "DeepSeek"),
        redis_url=_read_secret(args.redis_url_file, "Redis URL"),
        database_url=_read_secret(args.database_url_file, "database URL"),
    )
    try:
        return await qualify_text_corpus(
            corpus,
            gateway,
            mode="LIVE",
            corpus_sha256=digest,
            planned_cost_ceiling_usd=planned,
            maximum_planned_cost_usd=maximum,
        )
    finally:
        await gateway.close()
        await runtime.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = asyncio.run(_run(args))
        _write_report(args.output, report, overwrite=args.overwrite)
    except (OSError, ValueError) as exc:
        print(f"text qualification failed: {exc}")
        return 2
    print(f"Text corpus qualification: {report.status.value}; mode={report.mode}; live_orders_allowed=false")
    return 0 if report.status is QualificationStatus.PASS else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

"""Frozen labelled-news corpus qualification tests."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from kairos_llm import LLMResult, PriceTable, TokenUsage

from kairos_text.filter import LocalRelevanceFilter
from kairos_text.models import NewsItem
from kairos_text.prompts import SENTIMENT_SYSTEM
from kairos_text.schemas import SentimentBatch
from kairos_text.shadow_qualification import (
    HARD_MAXIMUM_PLANNED_COST_USD,
    QualificationStatus,
    TextCorpus,
    _live_gateway,
    _read_secret,
    _ScriptedGateway,
    load_corpus,
    main,
    planned_cost_ceiling_usd,
    qualify_text_corpus,
)

NOW = datetime(2026, 8, 26, 18, tzinfo=UTC)


async def test_packaged_text_corpus_passes_network_free_harness() -> None:
    corpus, digest = load_corpus()
    report = await qualify_text_corpus(
        corpus,
        _ScriptedGateway(),
        mode="STATIC_HARNESS",
        corpus_sha256=digest,
        maximum_planned_cost_usd=0.02,
        now=NOW,
    )

    assert report.status is QualificationStatus.PASS
    assert report.live_orders_allowed is False
    assert len(report.observations) == 10
    assert sum(item.model_called for item in report.observations) == 7
    assert all(item.deadline_met and item.attributable for item in report.observations)
    assert all(
        not item.actual_impacts
        for item in report.observations
        if item.category in {"prompt_injection", "stale", "future", "estimated_time"}
    )


async def test_targeted_case_replay_does_not_recall_passed_news() -> None:
    corpus, digest = load_corpus()
    report = await qualify_text_corpus(
        corpus,
        _ScriptedGateway(),
        mode="STATIC_HARNESS",
        corpus_sha256=digest,
        maximum_planned_cost_usd=0.02,
        now=NOW,
        selected_case_ids=("sol_official_etf_approval",),
    )
    assert [item.case_id for item in report.observations] == ["sol_official_etf_approval"]
    with pytest.raises(ValueError, match="unknown corpus case"):
        planned_cost_ceiling_usd(corpus, ("unknown",))


class _AlwaysBullishGateway:
    async def complete(self, **_kwargs) -> LLMResult:
        parsed = SentimentBatch.model_validate(
            {
                "signals": [
                    {
                        "topic": "fabricated",
                        "sentiment": 1,
                        "impact": "bullish",
                        "confidence": 1,
                        "summary": "unsafe",
                        "item_ids": [1],
                    }
                ]
            }
        )
        return LLMResult(
            content=parsed.model_dump_json(),
            parsed=parsed,
            model="deepseek-flash",
            effort="low",
            usage=TokenUsage(input_tokens=10, output_tokens=10),
            cost_usd=0.001,
            latency_s=0.1,
            workload="text_scouts",
            provider="deepseek",
            request_id="unsafe",
            resolved_model="deepseek-flash",
            budget_reservation_id="kairos-llm-v1:deepseek:unsafe",
        )


async def test_text_corpus_rejects_wrong_direction_and_prompt_injection_signal() -> None:
    corpus, digest = load_corpus()
    report = await qualify_text_corpus(
        corpus,
        _AlwaysBullishGateway(),
        mode="LIVE",
        corpus_sha256=digest,
        planned_cost_ceiling_usd=1,
        maximum_planned_cost_usd=1,
        now=NOW,
    )

    assert report.status is QualificationStatus.FAIL
    injection = next(item for item in report.observations if item.category == "prompt_injection")
    assert "prompt_injection_produced_signal" in injection.reasons
    bearish = next(item for item in report.observations if item.case_id.startswith("eth_"))
    assert "directional_label_mismatch" in bearish.reasons
    assert "provider_route_mismatch" in bearish.reasons
    assert "model_route_mismatch" in bearish.reasons


async def test_live_qualification_requires_expected_workload_and_paid_provenance() -> None:
    class WrongWorkloadGateway:
        async def complete(self, **_kwargs) -> LLMResult:
            parsed = SentimentBatch.model_validate({"signals": []})
            return LLMResult(
                content=parsed.model_dump_json(),
                parsed=parsed,
                model="gpt-6-luna",
                effort="low",
                workload="aggregator_normal",
                provider="openai",
                request_id="",
                resolved_model="gpt-6-luna",
                budget_reservation_id=None,
            )

    corpus, digest = load_corpus()
    report = await qualify_text_corpus(
        corpus,
        WrongWorkloadGateway(),
        mode="LIVE",
        corpus_sha256=digest,
        selected_case_ids=("btc_official_approval",),
        now=NOW,
    )

    observation = report.observations[0]
    assert "workload_mismatch" in observation.reasons
    assert "paid_provenance_missing" in observation.reasons


def test_corpus_requires_all_five_assets_and_unique_cases() -> None:
    corpus, _digest = load_corpus()
    payload = corpus.model_dump(mode="json")
    payload["cases"] = [item for item in payload["cases"] if not item["case_id"].startswith("xrp_")]
    with pytest.raises(ValueError, match="five active assets"):
        TextCorpus.model_validate(payload)

    payload = corpus.model_dump(mode="json")
    payload["cases"][1]["case_id"] = payload["cases"][0]["case_id"]
    with pytest.raises(ValueError, match="unique"):
        TextCorpus.model_validate(payload)


def test_local_filter_and_prompt_cover_the_five_asset_universe() -> None:
    selected = LocalRelevanceFilter(threshold=2, top_k=5).select(
        [
            NewsItem(title="Solana update"),
            NewsItem(title="BNB update"),
            NewsItem(title="Ripple XRP update"),
        ]
    )
    assert len(selected) == 3
    for asset in ("BTC", "ETH", "SOL", "BNB", "XRP"):
        assert asset in SENTIMENT_SYSTEM


def test_planned_cost_and_static_cli_are_bounded_and_sanitized(tmp_path: Path) -> None:
    corpus, _digest = load_corpus()
    planned = planned_cost_ceiling_usd(corpus)
    assert 0 < planned < HARD_MAXIMUM_PLANNED_COST_USD

    output = tmp_path / "text.json"
    assert main(["--static", "--output", str(output)]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["live_orders_allowed"] is False
    rendered = json.dumps(payload).casefold()
    assert "official.example.invalid" not in rendered
    assert "execute a long" not in rendered
    assert main(["--static", "--output", str(output)]) == 2


def test_planned_cost_reserves_the_active_text_route(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, TokenUsage]] = []

    def reserve(_self: PriceTable, model: str, usage: TokenUsage) -> float:
        seen.append((model, usage))
        return 0.001

    monkeypatch.setattr(PriceTable, "reservation_cost", reserve)
    corpus, _digest = load_corpus()
    expected_calls = sum(case.expected_model_call for case in corpus.cases)

    assert planned_cost_ceiling_usd(corpus) == pytest.approx(expected_calls * 0.001)
    assert len(seen) == expected_calls
    assert all(model == "gpt-6-luna" for model, _usage in seen)
    assert all(usage.input_tokens > 0 and usage.output_tokens == 512 for _model, usage in seen)


def test_static_mode_rejects_secret_files_before_reading(tmp_path: Path) -> None:
    assert (
        main(
            [
                "--static",
                "--deepseek-key-file",
                str(tmp_path / "missing"),
                "--output",
                str(tmp_path / "report.json"),
            ]
        )
        == 2
    )


def test_legacy_deepseek_flag_is_rejected_without_reading_or_running(tmp_path: Path, capsys) -> None:
    assert (
        main(
            [
                "--deepseek-key-file",
                str(tmp_path / "must-not-be-read"),
                "--output",
                str(tmp_path / "report.json"),
            ]
        )
        == 2
    )
    assert "no longer supported" in capsys.readouterr().out
    assert not (tmp_path / "report.json").exists()


def test_live_cli_requires_explicit_openai_credential_file(tmp_path: Path, capsys) -> None:
    assert main(["--output", str(tmp_path / "report.json")]) == 2
    assert "requires OpenAI, Redis and database" in capsys.readouterr().out
    assert not (tmp_path / "report.json").exists()


def test_openai_credential_file_must_be_one_nonempty_line(tmp_path: Path) -> None:
    secret_path = tmp_path / "test-openai-key.txt"
    secret_path.write_text("synthetic-test-key\n", encoding="utf-8")
    assert _read_secret(secret_path, "OpenAI") == "synthetic-test-key"
    secret_path.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="exactly one non-empty line"):
        _read_secret(secret_path, "OpenAI")
    secret_path.write_text("line-one\nline-two\n", encoding="utf-8")
    with pytest.raises(ValueError, match="exactly one non-empty line"):
        _read_secret(secret_path, "OpenAI")


def test_live_gateway_pins_openai_route_and_ignores_ambient_provider_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kairos_llm import LLMWorkload, Provider

    import kairos_text.shadow_qualification as qualification

    captured: dict[str, object] = {}

    class Runtime:
        async def close(self) -> None:
            return None

    class Gateway:
        def __init__(self, settings, *, router):
            captured["settings"] = settings
            captured["router"] = router

    class BudgetedGateway:
        def __init__(self, gateway, _budget, *, monthly_budgets_microusd):
            captured["gateway"] = gateway
            captured["monthly_budgets"] = monthly_budgets_microusd

    monkeypatch.setenv("KAIROS_DEEPSEEK_API_KEY", "ambient-deepseek-test-value")
    monkeypatch.setenv("KAIROS_OPENAI_API_KEY", "ambient-openai-test-value")
    monkeypatch.setenv("KAIROS_OPENAI_BASE_URL", "https://ambient.invalid")
    monkeypatch.setattr(qualification, "TextSettings", lambda **_kwargs: object())
    monkeypatch.setattr(qualification, "build_bus", lambda _settings: object())
    monkeypatch.setattr(qualification, "DurableMessageBus", lambda *args, **_kwargs: Runtime())
    monkeypatch.setattr(qualification, "DurableLLMUsageBudget", lambda _runtime: object())
    monkeypatch.setattr(qualification, "LLMGateway", Gateway)
    monkeypatch.setattr(qualification, "BudgetedLLMGateway", BudgetedGateway)

    budgeted, runtime = asyncio.run(
        _live_gateway(
            openai_key="explicit-test-openai-key",
            redis_url="redis://127.0.0.1:6379/0",
            database_url="postgresql://test:test@127.0.0.1/test",
        )
    )

    settings = captured["settings"]
    router = captured["router"]
    route = router.resolve(workload=LLMWorkload.TEXT_SCOUTS)
    assert settings.openai_api_key == "explicit-test-openai-key"
    assert settings.openai_base_url is None
    assert settings.deepseek_api_key == ""
    assert settings.max_retries == 0
    assert route.choice.provider is Provider.OPENAI
    assert route.choice.model == "gpt-6-luna"
    assert budgeted is not None and runtime is not None


def test_static_cli_remains_credential_free_with_ambient_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kairos_text.shadow_qualification as qualification

    monkeypatch.setenv("KAIROS_OPENAI_API_KEY", "ambient-openai-test-value")
    monkeypatch.setenv("KAIROS_DEEPSEEK_API_KEY", "ambient-deepseek-test-value")

    async def forbidden_live_gateway(**_kwargs):
        raise AssertionError("static qualification attempted live gateway construction")

    monkeypatch.setattr(qualification, "_live_gateway", forbidden_live_gateway)
    assert main(["--static", "--output", str(tmp_path / "static.json")]) == 0
    payload = json.loads((tmp_path / "static.json").read_text(encoding="utf-8"))
    assert {item["provider"] for item in payload["observations"] if item["provider"]} == {"offline"}

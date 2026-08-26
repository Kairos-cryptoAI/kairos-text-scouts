"""Frozen labelled-news corpus qualification tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from kairos_llm import LLMResult, TokenUsage

from kairos_text.filter import LocalRelevanceFilter
from kairos_text.models import NewsItem
from kairos_text.prompts import SENTIMENT_SYSTEM
from kairos_text.schemas import SentimentBatch
from kairos_text.shadow_qualification import (
    HARD_MAXIMUM_PLANNED_COST_USD,
    QualificationStatus,
    TextCorpus,
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
        selected_case_ids=("sol_official_outage_recovery",),
    )
    assert [item.case_id for item in report.observations] == ["sol_official_outage_recovery"]
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
            model="deepseek-v4-flash",
            effort="low",
            usage=TokenUsage(input_tokens=10, output_tokens=10),
            cost_usd=0.001,
            latency_s=0.1,
            workload="text_scouts",
            provider="deepseek",
            request_id="unsafe",
            resolved_model="deepseek-v4-flash",
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

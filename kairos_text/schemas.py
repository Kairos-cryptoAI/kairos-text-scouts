"""Strict model output contracts for sentiment extraction."""

from __future__ import annotations

from kairos_core.enums import ImpactDirection
from pydantic import BaseModel, ConfigDict, Field


class ExtractedSentiment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1, max_length=96)
    sentiment: float = Field(ge=-1.0, le=1.0)
    impact: ImpactDirection
    confidence: float = Field(ge=0.0, le=1.0)
    summary: str = Field(max_length=500)
    item_ids: list[int] = Field(min_length=1, max_length=20)


class SentimentBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signals: list[ExtractedSentiment] = Field(max_length=20)

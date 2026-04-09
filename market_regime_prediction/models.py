from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from models.signals import SignalCandidate


def _normalize_direction(value: str) -> str:
    return value.upper().strip()


class SymbolMarketFeatures(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timestamp: int
    close: float
    return_pct: float
    ema20: float
    ema50: float
    above_ema20: bool
    above_ema50: bool
    trend_score: float
    relative_strength_vs_btc: float
    atr_pct: float
    bb_width: float

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: int) -> int:
        if value < 0:
            raise ValueError("timestamp must be non-negative")
        return value


class LiveMarketContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timestamp: int
    fresh_count: int
    total_tracked_symbols: int
    coverage_ratio: float = Field(ge=0.0, le=1.0)
    btc_symbol: str
    btc_present: bool
    confidence: float = Field(ge=0.0, le=1.0)
    is_provisional: bool
    advancers: int
    decliners: int
    advancers_ratio: float = Field(ge=0.0, le=1.0)
    decliners_ratio: float = Field(ge=0.0, le=1.0)
    advancers_decliners_ratio: float = Field(ge=0.0)
    average_return: float
    average_relative_strength_vs_btc: float
    pct_above_ema20: float = Field(ge=0.0, le=1.0)
    pct_above_ema50: float = Field(ge=0.0, le=1.0)
    average_trend_score: float
    average_atr_pct: float = Field(ge=0.0)
    average_bb_width: float = Field(ge=0.0)
    btc_return: float
    btc_trend_score: float
    btc_regime_score: float = Field(ge=-1.0, le=1.0)
    market_stress_score: float = Field(ge=0.0, le=1.0)
    long_tailwind: float = Field(ge=-1.0, le=1.0)
    short_tailwind: float = Field(ge=-1.0, le=1.0)
    symbol_features: dict[str, SymbolMarketFeatures] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("btc_symbol")
    @classmethod
    def validate_btc_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator(
        "timestamp", "fresh_count", "total_tracked_symbols", "advancers", "decliners"
    )
    @classmethod
    def validate_non_negative_ints(cls, value: int) -> int:
        if value < 0:
            raise ValueError("value must be non-negative")
        return value

    @property
    def is_full(self) -> bool:
        return not self.is_provisional

    @model_validator(mode="after")
    def validate_consistency(self) -> LiveMarketContext:
        if self.fresh_count > self.total_tracked_symbols:
            raise ValueError("fresh_count cannot exceed total_tracked_symbols")
        if self.advancers + self.decliners > self.fresh_count:
            raise ValueError("advancers plus decliners cannot exceed fresh_count")
        return self


class MarketContextScore(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    direction: str
    context_timestamp: int | None
    confidence: float = Field(ge=0.0, le=1.0)
    long_tailwind: float = Field(ge=-1.0, le=1.0)
    short_tailwind: float = Field(ge=-1.0, le=1.0)
    breadth_score: float = Field(ge=-1.0, le=1.0)
    btc_alignment_score: float = Field(ge=-1.0, le=1.0)
    cross_asset_confirmation: float = Field(ge=-1.0, le=1.0)
    market_stress_score: float = Field(ge=0.0, le=1.0)
    followthrough_score: float = Field(ge=-1.0, le=1.0)
    adverse_excursion_risk: float = Field(ge=0.0, le=1.0)
    override_strength: float = Field(ge=0.0, le=1.0)
    supportiveness_score: float = Field(ge=-1.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, value: str) -> str:
        return _normalize_direction(value)

    @field_validator("context_timestamp")
    @classmethod
    def validate_context_timestamp(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("context_timestamp must be non-negative")
        return value


class SignalContextEvaluation(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    candidate: SignalCandidate
    adjusted_score: float
    emit: bool = Field(default=True)
    context_score: MarketContextScore

from __future__ import annotations
from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


MarketRegime: TypeAlias = Literal[
    "TREND_UP",
    "TREND_DOWN",
    "RANGE",
    "HIGH_STRESS",
    "TRANSITIONAL",
]

MicroRegime: TypeAlias = Literal[
    "TREND_UP",
    "TREND_DOWN",
    "RANGE",
    "VOLATILE",
    "TRANSITIONAL",
]

MarketRegimeTransition: TypeAlias = Literal[
    "STRESS_SPIKE",
    "STRESS_RELIEF",
    "ENTERED_TREND_UP",
    "ENTERED_TREND_DOWN",
    "ENTERED_RANGE",
    "LOST_REGIME_EDGE",
]

MicroRegimeTransition: TypeAlias = Literal[
    "VOLATILITY_EXPANSION",
    "BREAKOUT_UP",
    "BREAKDOWN",
    "RECOVERY",
    "MEAN_REVERSION",
    "ENTERED_TREND_UP",
    "ENTERED_TREND_DOWN",
    "ENTERED_RANGE",
    "ENTERED_TRANSITIONAL",
]

PositioningState: TypeAlias = Literal[
    "NEUTRAL",
    "NEW_LEVERAGE_LONG",
    "NEW_LEVERAGE_SHORT",
    "SHORT_SQUEEZE",
    "DELEVERAGING_FLUSH",
    "CASCADE_RISK",
]


def _normalize_direction(value: str) -> str:
    return value.upper().strip()


def _canonicalize_symbol(value: str) -> str:
    return value.upper().strip().replace("-", "").replace("_", "")


class DerivativesPositioningFeatures(BaseModel):
    """Point-in-time derivatives positioning attached to one symbol."""

    model_config = ConfigDict(extra="forbid")

    timestamp: int
    open_interest: float = Field(ge=0.0)
    open_interest_notional: float = Field(ge=0.0)
    oi_change_5m: float | None = None
    oi_change_15m: float | None = None
    oi_change_1h: float | None = None
    oi_zscore: float | None = None
    current_funding_rate: float | None = None
    annualized_funding_rate: float | None = None
    funding_percentile: float | None = Field(default=None, ge=0.0, le=1.0)
    funding_dispersion: float | None = Field(default=None, ge=0.0)
    funding_rates_by_exchange: dict[str, float] = Field(default_factory=dict)
    mark_index_basis_bps: float | None = None
    long_liquidation_notional: float = Field(default=0.0, ge=0.0)
    short_liquidation_notional: float = Field(default=0.0, ge=0.0)
    liquidation_intensity: float | None = Field(default=None, ge=0.0)
    stablecoin_margined_oi: float | None = Field(default=None, ge=0.0)
    coin_margined_oi: float | None = Field(default=None, ge=0.0)
    oi_to_volume_ratio: float | None = Field(default=None, ge=0.0)
    derivatives_stress_score: float = Field(default=0.0, ge=0.0, le=1.0)
    positioning_state: PositioningState = "NEUTRAL"
    source: str = "kucoin"
    liquidation_source: str | None = None

    @field_validator("timestamp")
    @classmethod
    def validate_positioning_timestamp(cls, value: int) -> int:
        if value < 0:
            raise ValueError("timestamp must be non-negative")
        return value


class LiquidationEvent(BaseModel):
    """One normalized forced-position close from a public liquidation feed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    timestamp: int = Field(ge=0)
    symbol: str
    position_side: Literal["LONG", "SHORT"]
    notional: float = Field(gt=0.0)

    @field_validator("symbol")
    @classmethod
    def validate_liquidation_symbol(cls, value: str) -> str:
        normalized = value.upper().strip()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized


class LiquidationWindow(BaseModel):
    """Aggregated long and short liquidations over one rolling window."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    long_notional: float = Field(default=0.0, ge=0.0)
    short_notional: float = Field(default=0.0, ge=0.0)

    @property
    def total_notional(self) -> float:
        return self.long_notional + self.short_notional


class SymbolMarketFeatures(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timestamp: int
    close: float
    return_pct: float
    # Multi-bar return over RELATIVE_STRENGTH_HORIZON_BARS. `return_pct` is a
    # single-bar return, which is far too noisy to judge sustained strength.
    return_pct_horizon: float = 0.0
    ema20: float
    ema50: float
    above_ema20: bool
    above_ema50: bool
    trend_score: float
    # Single-bar spread vs BTC. Useful for "is it outperforming right now",
    # but not for whether a coin has been sustainably stronger than BTC.
    relative_strength_vs_btc: float
    # Spread vs BTC measured over RELATIVE_STRENGTH_HORIZON_BARS. Strategies
    # whose thesis is a multi-hour move must use this one.
    relative_strength_vs_btc_horizon: float = 0.0
    atr_pct: float
    bb_width: float
    derivatives: DerivativesPositioningFeatures | None = None
    micro_regime: MicroRegime | None = None
    micro_regime_strength: float = Field(default=0.0, ge=0.0, le=1.0)
    micro_regime_transition: MicroRegimeTransition | None = None
    micro_regime_transition_strength: float = Field(default=0.0, ge=0.0, le=1.0)

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
    market_regime: MarketRegime | None = None
    previous_market_regime: MarketRegime | None = None
    market_regime_transition: MarketRegimeTransition | None = None
    market_regime_transition_strength: float = Field(default=0.0, ge=0.0, le=1.0)
    long_regime_score: float = Field(default=0.0, ge=0.0, le=1.0)
    short_regime_score: float = Field(default=0.0, ge=0.0, le=1.0)
    range_regime_score: float = Field(default=0.0, ge=0.0, le=1.0)
    stress_regime_score: float = Field(default=0.0, ge=0.0, le=1.0)
    regime_is_transitioning: bool = False
    regime_stable_since: int | None = Field(
        default=None,
        description="Timestamp (ms) when the current market_regime was first entered; used to derive regime stability.",
    )
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

    def get_symbol_features(self, symbol: str) -> SymbolMarketFeatures | None:
        normalized = symbol.strip().upper()
        direct = self.symbol_features.get(normalized)
        if direct is not None:
            return direct

        canonical = _canonicalize_symbol(normalized)
        for known_symbol, features in self.symbol_features.items():
            if _canonicalize_symbol(known_symbol) == canonical:
                return features
        return None


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

    symbol: str
    direction: str
    local_score: float
    local_features: dict[str, float]
    adjusted_score: float
    emit: bool = Field(default=True)
    context_score: MarketContextScore

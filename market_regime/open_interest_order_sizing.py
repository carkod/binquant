from __future__ import annotations

from pybinbot import (
    BotBase,
    OpenInterestSizingDecision,
    OpenInterestSizingEvidence,
    Position,
)

from market_regime.models import DerivativesPositioningFeatures


BASELINE_MARGIN = 4.0
MODERATELY_SUPPORTIVE_MARGIN = 6.0
STRONGLY_SUPPORTIVE_MARGIN = 8.0
MODERATELY_ADVERSE_MARGIN = 3.0
STRONGLY_ADVERSE_MARGIN = 2.0
OI_CHANGE_FLOOR = 0.002
STRONG_OI_CHANGE = 0.01
MAX_SNAPSHOT_AGE_MS = 15 * 60 * 1000
MAX_SNAPSHOT_FUTURE_SKEW_MS = 5 * 60 * 1000

OI_SIZED_STRATEGIES = frozenset(
    {
        "coinrule_price_tracker",
        "failed_spike_fade",
        "liquidation_sweep_pump",
        "relative_strength_impulse_rider",
        "top_gainer_early_momentum",
    }
)


def calculate_open_interest_sizing(
    *,
    direction: Position | str,
    positioning: DerivativesPositioningFeatures | None,
    signal_timestamp: int,
) -> OpenInterestSizingDecision | None:
    if positioning is None:
        return None

    snapshot_age = signal_timestamp - positioning.timestamp
    if (
        snapshot_age < -MAX_SNAPSHOT_FUTURE_SKEW_MS
        or snapshot_age > MAX_SNAPSHOT_AGE_MS
    ):
        return None

    state = positioning.positioning_state
    oi_change = positioning.oi_change_15m
    if state == "CASCADE_RISK":
        return OpenInterestSizingDecision(
            baseline_margin=BASELINE_MARGIN,
            adjusted_margin=STRONGLY_ADVERSE_MARGIN,
            multiplier=STRONGLY_ADVERSE_MARGIN / BASELINE_MARGIN,
            oi_change_15m=oi_change,
            positioning_state=state,
            evidence="STRONGLY_ADVERSE",
            snapshot_timestamp=positioning.timestamp,
        )

    if oi_change is None or abs(oi_change) < OI_CHANGE_FLOOR:
        return None

    normalized_direction = Position(direction)
    supportive_states = (
        {"NEW_LEVERAGE_LONG", "SHORT_SQUEEZE"}
        if normalized_direction == Position.long
        else {"NEW_LEVERAGE_SHORT", "DELEVERAGING_FLUSH"}
    )
    adverse_states = (
        {"NEW_LEVERAGE_SHORT", "DELEVERAGING_FLUSH"}
        if normalized_direction == Position.long
        else {"NEW_LEVERAGE_LONG", "SHORT_SQUEEZE"}
    )
    is_strong = abs(oi_change) >= STRONG_OI_CHANGE
    evidence: OpenInterestSizingEvidence

    if state in supportive_states:
        margin = (
            STRONGLY_SUPPORTIVE_MARGIN if is_strong else MODERATELY_SUPPORTIVE_MARGIN
        )
        evidence = "STRONGLY_SUPPORTIVE" if is_strong else "MODERATELY_SUPPORTIVE"
    elif state in adverse_states:
        margin = STRONGLY_ADVERSE_MARGIN if is_strong else MODERATELY_ADVERSE_MARGIN
        evidence = "STRONGLY_ADVERSE" if is_strong else "MODERATELY_ADVERSE"
    else:
        return None

    return OpenInterestSizingDecision(
        baseline_margin=BASELINE_MARGIN,
        adjusted_margin=margin,
        multiplier=margin / BASELINE_MARGIN,
        oi_change_15m=oi_change,
        positioning_state=state,
        evidence=evidence,
        snapshot_timestamp=positioning.timestamp,
    )


def apply_open_interest_sizing(
    *,
    bot_params: BotBase,
    positioning: DerivativesPositioningFeatures | None,
    signal_timestamp: int,
) -> OpenInterestSizingDecision | None:
    bot_params.fiat_order_size = BASELINE_MARGIN
    decision = calculate_open_interest_sizing(
        direction=bot_params.position,
        positioning=positioning,
        signal_timestamp=signal_timestamp,
    )
    if decision is not None:
        bot_params.fiat_order_size = decision.adjusted_margin
    return decision


def activity_burst_derivatives_block_reason(
    positioning: DerivativesPositioningFeatures | None,
) -> str | None:
    if positioning is None:
        return None
    if (
        positioning.positioning_state == "CASCADE_RISK"
        or positioning.derivatives_stress_score >= 0.7
    ):
        return "derivatives_cascade_risk"

    oi_change = positioning.oi_change_15m
    if oi_change is None or oi_change < STRONG_OI_CHANGE:
        return None

    crowded_oi = positioning.oi_zscore is not None and positioning.oi_zscore >= 2.0
    crowded_funding = (
        positioning.annualized_funding_rate is not None
        and positioning.annualized_funding_rate >= 0.50
    )
    crowded_percentile = (
        positioning.funding_percentile is not None
        and positioning.funding_percentile >= 0.90
    )
    if crowded_oi or crowded_funding or crowded_percentile:
        return "crowded_oi_expansion"
    return None

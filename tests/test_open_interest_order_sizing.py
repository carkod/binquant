import pytest
from pybinbot import BotBase, Position

from market_regime.models import DerivativesPositioningFeatures
from market_regime.open_interest_order_sizing import (
    BASELINE_MARGIN,
    activity_burst_derivatives_block_reason,
    apply_open_interest_sizing,
    calculate_open_interest_sizing,
)


NOW = 1_700_000_000_000


def positioning(
    *,
    state: str,
    oi_change_15m: float | None,
    timestamp: int = NOW,
    stress: float = 0.0,
    oi_zscore: float | None = None,
    annualized_funding: float | None = None,
) -> DerivativesPositioningFeatures:
    return DerivativesPositioningFeatures(
        timestamp=timestamp,
        open_interest=1_000.0,
        open_interest_notional=100_000.0,
        oi_change_15m=oi_change_15m,
        oi_zscore=oi_zscore,
        annualized_funding_rate=annualized_funding,
        derivatives_stress_score=stress,
        positioning_state=state,
    )


@pytest.mark.parametrize(
    ("oi_change", "expected_margin", "expected_evidence"),
    [
        (0.005, 6.0, "MODERATELY_SUPPORTIVE"),
        (0.01, 8.0, "STRONGLY_SUPPORTIVE"),
    ],
)
def test_long_supportive_oi_increases_margin(
    oi_change: float,
    expected_margin: float,
    expected_evidence: str,
) -> None:
    decision = calculate_open_interest_sizing(
        direction=Position.long,
        positioning=positioning(
            state="NEW_LEVERAGE_LONG",
            oi_change_15m=oi_change,
        ),
        signal_timestamp=NOW,
    )

    assert decision is not None
    assert decision.adjusted_margin == expected_margin
    assert decision.evidence == expected_evidence


@pytest.mark.parametrize(
    ("oi_change", "expected_margin", "expected_evidence"),
    [
        (0.005, 3.0, "MODERATELY_ADVERSE"),
        (0.01, 2.0, "STRONGLY_ADVERSE"),
    ],
)
def test_long_adverse_oi_reduces_margin(
    oi_change: float,
    expected_margin: float,
    expected_evidence: str,
) -> None:
    decision = calculate_open_interest_sizing(
        direction=Position.long,
        positioning=positioning(
            state="NEW_LEVERAGE_SHORT",
            oi_change_15m=oi_change,
        ),
        signal_timestamp=NOW,
    )

    assert decision is not None
    assert decision.adjusted_margin == expected_margin
    assert decision.evidence == expected_evidence


def test_short_uses_inverse_directional_states() -> None:
    decision = calculate_open_interest_sizing(
        direction=Position.short,
        positioning=positioning(
            state="NEW_LEVERAGE_SHORT",
            oi_change_15m=0.012,
        ),
        signal_timestamp=NOW,
    )

    assert decision is not None
    assert decision.adjusted_margin == 8.0


def test_neutral_or_stale_positioning_keeps_four_usdt_baseline() -> None:
    bot = BotBase(
        pair="TESTUSDTM",
        position=Position.long,
        fiat_order_size=1.33,
    )

    decision = apply_open_interest_sizing(
        bot_params=bot,
        positioning=positioning(
            state="NEW_LEVERAGE_LONG",
            oi_change_15m=0.02,
            timestamp=NOW - 16 * 60 * 1000,
        ),
        signal_timestamp=NOW,
    )

    assert decision is None
    assert bot.fiat_order_size == BASELINE_MARGIN
    assert bot.logs == []


def test_supportive_sizing_changes_margin_without_changing_bot_logs() -> None:
    bot = BotBase(
        pair="TESTUSDTM",
        position=Position.long,
    )

    decision = apply_open_interest_sizing(
        bot_params=bot,
        positioning=positioning(
            state="NEW_LEVERAGE_LONG",
            oi_change_15m=0.012,
        ),
        signal_timestamp=NOW,
    )

    assert decision is not None
    assert bot.fiat_order_size == 8.0
    assert bot.logs == []
    assert "logs" not in bot.model_fields_set


def test_cascade_risk_uses_two_usdt_even_without_oi_change() -> None:
    decision = calculate_open_interest_sizing(
        direction=Position.long,
        positioning=positioning(
            state="CASCADE_RISK",
            oi_change_15m=None,
            stress=0.8,
        ),
        signal_timestamp=NOW,
    )

    assert decision is not None
    assert decision.adjusted_margin == 2.0
    assert decision.evidence == "STRONGLY_ADVERSE"


def test_activity_burst_blocks_only_cascade_or_crowded_strong_expansion() -> None:
    assert (
        activity_burst_derivatives_block_reason(
            positioning(
                state="CASCADE_RISK",
                oi_change_15m=None,
                stress=0.8,
            )
        )
        == "derivatives_cascade_risk"
    )
    assert (
        activity_burst_derivatives_block_reason(
            positioning(
                state="NEW_LEVERAGE_LONG",
                oi_change_15m=0.015,
                oi_zscore=2.1,
            )
        )
        == "crowded_oi_expansion"
    )
    assert (
        activity_burst_derivatives_block_reason(
            positioning(
                state="SHORT_SQUEEZE",
                oi_change_15m=-0.02,
                oi_zscore=2.1,
            )
        )
        is None
    )

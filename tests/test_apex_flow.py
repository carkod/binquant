from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from strategies.apex_flow import ApexFlow
from market_regime.models import LiveMarketContext
from market_regime.regime_transitions import RegimeTransitionDetector


def make_live_context(
    *,
    timestamp: int,
    advancers_ratio: float,
    confidence: float,
    long_tailwind: float,
    short_tailwind: float,
    btc_regime_score: float,
    market_stress_score: float,
) -> LiveMarketContext:
    return LiveMarketContext(
        timestamp=timestamp,
        fresh_count=45,
        total_tracked_symbols=45,
        coverage_ratio=1.0,
        btc_symbol="BTCUSDT",
        btc_present=True,
        confidence=confidence,
        is_provisional=False,
        advancers=round(45 * advancers_ratio),
        decliners=45 - round(45 * advancers_ratio),
        advancers_ratio=advancers_ratio,
        decliners_ratio=1.0 - advancers_ratio,
        advancers_decliners_ratio=(advancers_ratio / max(1.0 - advancers_ratio, 1e-9)),
        average_return=0.012,
        average_relative_strength_vs_btc=0.01,
        pct_above_ema20=advancers_ratio,
        pct_above_ema50=advancers_ratio,
        average_trend_score=0.15,
        average_atr_pct=0.02,
        average_bb_width=0.05,
        btc_return=0.01,
        btc_trend_score=0.2,
        btc_regime_score=btc_regime_score,
        market_stress_score=market_stress_score,
        long_tailwind=long_tailwind,
        short_tailwind=short_tailwind,
        symbol_features={},
        metadata={},
    )


def annotate_context(
    context: LiveMarketContext,
    previous_context: LiveMarketContext | None = None,
) -> LiveMarketContext:
    return RegimeTransitionDetector().annotate_context(
        context=context,
        previous_context=previous_context,
    )


def make_algo(
    context: LiveMarketContext,
    *,
    last_market_regime: str | None = None,
) -> ApexFlow:
    cls = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        telegram_consumer=SimpleNamespace(send_signal=AsyncMock()),
        latest_market_context=context,
        last_market_regime=last_market_regime,
    )
    return ApexFlow(cast(Any, cls))


@pytest.mark.asyncio
async def test_apex_flow_bootstraps_without_emitting_transition():
    algo = make_algo(
        annotate_context(
            make_live_context(
                timestamp=1_000,
                advancers_ratio=0.67,
                confidence=0.95,
                long_tailwind=0.8,
                short_tailwind=-0.2,
                btc_regime_score=0.7,
                market_stress_score=0.18,
            )
        )
    )

    await algo.signal()

    algo.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_apex_flow_emits_long_to_short_transition():
    first_context = annotate_context(
        make_live_context(
            timestamp=1_000,
            advancers_ratio=0.68,
            confidence=0.95,
            long_tailwind=0.75,
            short_tailwind=-0.1,
            btc_regime_score=0.65,
            market_stress_score=0.2,
        )
    )
    second_context = annotate_context(
        make_live_context(
            timestamp=2_000,
            advancers_ratio=0.29,
            confidence=0.94,
            long_tailwind=-0.45,
            short_tailwind=0.82,
            btc_regime_score=-0.72,
            market_stress_score=0.64,
        ),
        previous_context=first_context,
    )
    first = make_algo(first_context)
    second = make_algo(second_context)

    await first.signal()
    await second.signal()

    second.telegram_consumer.send_signal.assert_awaited_once()  # type: ignore[attr-defined]
    await_args = second.telegram_consumer.send_signal.await_args  # type: ignore[attr-defined]
    assert await_args is not None
    sent_message = await_args.args[0]
    assert "Regime transition: TREND_UP -> HIGH_STRESS" in sent_message
    assert "#market_regime_transition" in sent_message


@pytest.mark.asyncio
async def test_apex_flow_emits_transition_into_neutral_regime():
    first_context = annotate_context(
        make_live_context(
            timestamp=1_000,
            advancers_ratio=0.66,
            confidence=0.96,
            long_tailwind=0.7,
            short_tailwind=-0.1,
            btc_regime_score=0.6,
            market_stress_score=0.2,
        )
    )
    second_context = annotate_context(
        make_live_context(
            timestamp=2_000,
            advancers_ratio=0.51,
            confidence=0.92,
            long_tailwind=0.1,
            short_tailwind=0.12,
            btc_regime_score=0.02,
            market_stress_score=0.41,
        ),
        previous_context=first_context,
    )
    first = make_algo(first_context)
    second = make_algo(second_context)

    await first.signal()
    await second.signal()

    second.telegram_consumer.send_signal.assert_awaited_once()  # type: ignore[attr-defined]
    await_args = second.telegram_consumer.send_signal.await_args  # type: ignore[attr-defined]
    assert await_args is not None
    sent_message = await_args.args[0]
    assert "Regime transition: TREND_UP -> RANGE" in sent_message
    assert "mean-reversion and range trading" in sent_message


@pytest.mark.asyncio
async def test_apex_flow_emits_from_annotated_context_without_bootstrap_state():
    first_context = annotate_context(
        make_live_context(
            timestamp=1_000,
            advancers_ratio=0.67,
            confidence=0.95,
            long_tailwind=0.78,
            short_tailwind=-0.12,
            btc_regime_score=0.66,
            market_stress_score=0.2,
        )
    )
    transitioned_context = annotate_context(
        make_live_context(
            timestamp=2_000,
            advancers_ratio=0.35,
            confidence=0.93,
            long_tailwind=-0.12,
            short_tailwind=0.35,
            btc_regime_score=-0.18,
            market_stress_score=0.22,
        ),
        previous_context=first_context,
    )

    algo = make_algo(transitioned_context)

    await algo.signal()

    algo.telegram_consumer.send_signal.assert_awaited_once()  # type: ignore[attr-defined]
    await_args = algo.telegram_consumer.send_signal.await_args  # type: ignore[attr-defined]
    assert await_args is not None
    sent_message = await_args.args[0]
    assert "Regime transition:" in sent_message

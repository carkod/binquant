from pybinbot import Strategy

from market_regime_prediction.models import LiveMarketContext
from producers.context_evaluator import ContextEvaluator


def make_context(
    *,
    advancers_ratio: float,
    market_stress_score: float,
) -> LiveMarketContext:
    return LiveMarketContext(
        timestamp=0,
        fresh_count=10,
        total_tracked_symbols=10,
        coverage_ratio=1.0,
        btc_symbol="BTCUSDT",
        btc_present=True,
        confidence=1.0,
        is_provisional=False,
        advancers=6,
        decliners=4,
        advancers_ratio=advancers_ratio,
        decliners_ratio=1.0 - advancers_ratio,
        advancers_decliners_ratio=1.5,
        average_return=0.01,
        average_relative_strength_vs_btc=0.0,
        pct_above_ema20=0.6,
        pct_above_ema50=0.6,
        average_trend_score=0.2,
        average_atr_pct=0.03,
        average_bb_width=0.09,
        btc_return=0.0,
        btc_trend_score=0.0,
        btc_regime_score=0.0,
        market_stress_score=market_stress_score,
        long_tailwind=0.2,
        short_tailwind=-0.1,
        symbol_features={},
        metadata={},
    )


def make_evaluator(context: LiveMarketContext | None) -> ContextEvaluator:
    evaluator = ContextEvaluator.__new__(ContextEvaluator)
    evaluator.latest_market_context = context
    evaluator._breadth_cross_tolerance = 0.05
    evaluator._autotrade_stress_threshold = 0.35
    return evaluator


def test_should_autotrade_blocks_when_market_stress_is_high():
    evaluator = make_evaluator(
        make_context(advancers_ratio=0.7, market_stress_score=0.4)
    )

    assert evaluator.should_autotrade(Strategy.long, True) is False


def test_should_autotrade_allows_when_bias_matches_and_stress_is_low():
    evaluator = make_evaluator(
        make_context(advancers_ratio=0.7, market_stress_score=0.2)
    )

    assert evaluator.should_autotrade(Strategy.long, True) is True


def test_should_autotrade_blocks_when_bias_conflicts_even_if_stress_is_low():
    evaluator = make_evaluator(
        make_context(advancers_ratio=0.7, market_stress_score=0.2)
    )

    assert evaluator.should_autotrade(Strategy.margin_short, True) is False

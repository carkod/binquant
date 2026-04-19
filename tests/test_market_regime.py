from market_regime.live_market_context_accumulator import (
    LiveMarketContextAccumulator,
)
from market_regime.context_scoring import RuleBasedMarketContextModel
from market_regime.market_state_store import MarketStateStore
from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import (
    allows_long_autotrade,
    supports_grid_trading,
)
from market_regime.regime_transitions import RegimeTransitionDetector
from market_regime.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime.signal_context_scorer import SignalContextScorer


def make_candle(timestamp: int, close: float) -> dict[str, float]:
    return {
        "timestamp": timestamp,
        "open": close * 0.99,
        "high": close * 1.01,
        "low": close * 0.98,
        "close": close,
        "volume": 1000.0,
    }


def seed_symbol(
    store: MarketStateStore,
    symbol: str,
    previous_timestamp: int,
    previous_close: float,
) -> None:
    store.update(symbol, make_candle(previous_timestamp, previous_close))


def make_symbol_features(**overrides: object) -> SymbolMarketFeatures:
    values: dict[str, object] = {
        "symbol": "ALT0USDT",
        "timestamp": 2_000,
        "close": 100.0,
        "return_pct": 0.01,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.01,
        "relative_strength_vs_btc": 0.02,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "RANGE",
        "micro_regime_strength": 0.7,
        "micro_regime_transition": None,
        "micro_regime_transition_strength": 0.0,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_live_context(**overrides: object) -> LiveMarketContext:
    values: dict[str, object] = {
        "timestamp": 2_000,
        "fresh_count": 40,
        "total_tracked_symbols": 40,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "btc_present": True,
        "confidence": 1.0,
        "is_provisional": False,
        "advancers": 30,
        "decliners": 10,
        "advancers_ratio": 0.75,
        "decliners_ratio": 0.25,
        "advancers_decliners_ratio": 3.0,
        "average_return": 0.01,
        "average_relative_strength_vs_btc": 0.01,
        "pct_above_ema20": 0.5,
        "pct_above_ema50": 0.5,
        "average_trend_score": 0.05,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.01,
        "btc_trend_score": 0.01,
        "btc_regime_score": -1.0,
        "market_stress_score": 0.2,
        "long_tailwind": -1.0,
        "short_tailwind": 0.4,
        "market_regime": None,
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.0,
        "short_regime_score": 0.0,
        "range_regime_score": 0.0,
        "stress_regime_score": 0.0,
        "regime_is_transitioning": False,
        "symbol_features": {"ALT0USDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def test_live_context_requires_threshold_without_fresh_btc() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")

    context = None
    for index in range(40):
        symbol = f"ALT{index}USDT"
        seed_symbol(store, symbol, 1_000, 100 + index)
        context = accumulator.on_closed_candle(symbol, make_candle(2_000, 101 + index))

    assert context is not None
    assert context.fresh_count == 40
    assert context.confidence == 1.0
    assert context.btc_present is False
    assert context.btc_regime_score == 0.0


def test_live_context_uses_stale_btc_when_available() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")

    seed_symbol(store, "BTCUSDT", 1_000, 200.0)
    store.update("BTCUSDT", make_candle(1_500, 202.0))

    context = None
    for index in range(40):
        symbol = f"ALT{index}USDT"
        seed_symbol(store, symbol, 1_000, 100 + index)
        context = accumulator.on_closed_candle(symbol, make_candle(2_000, 101 + index))

    assert context is not None
    assert context.btc_present is True
    assert context.metadata["btc_fresh"] is False
    assert context.metadata["btc_used_for_regime"] is True


def test_live_context_recomputes_same_timestamp() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")
    symbols = ["BTCUSDT"] + [f"ALT{index}USDT" for index in range(44)]

    for index, symbol in enumerate(symbols):
        seed_symbol(store, symbol, 1_000, 100 + index)

    context = None
    for index, symbol in enumerate(symbols[:45]):
        context = accumulator.on_closed_candle(symbol, make_candle(2_000, 101 + index))

    assert context is not None
    assert context.timestamp == 2_000
    assert context.fresh_count == 45
    assert context.confidence == 1.0
    assert context.market_regime in {"TREND_UP", "RANGE", "TRANSITIONAL", "HIGH_STRESS"}


def test_stale_symbols_are_not_mixed_into_next_timestamp() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")
    symbols = ["BTCUSDT"] + [f"ALT{index}USDT" for index in range(39)]

    for index, symbol in enumerate(symbols):
        seed_symbol(store, symbol, 1_000, 100 + index)
        accumulator.on_closed_candle(symbol, make_candle(2_000, 101 + index))

    accumulator.on_closed_candle("BTCUSDT", make_candle(3_000, 205.0))
    accumulator.on_closed_candle("ALT0USDT", make_candle(3_000, 105.0))

    assert accumulator.get_context(3_000) is None


def test_signal_context_scorer_penalizes_weak_long() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")
    symbols = ["BTCUSDT"] + [f"ALT{index}USDT" for index in range(39)]
    prices = {
        "BTCUSDT": (200.0, 180.0),
        "ALT0USDT": (100.0, 95.0),
        "ALT1USDT": (100.0, 94.0),
        "ALT2USDT": (100.0, 93.0),
        "ALT3USDT": (100.0, 92.0),
        "ALT4USDT": (100.0, 91.0),
        "ALT5USDT": (100.0, 90.0),
        "ALT6USDT": (100.0, 89.0),
        "ALT7USDT": (100.0, 88.0),
        "ALT8USDT": (100.0, 87.0),
    }
    for index in range(9, 39):
        prices[f"ALT{index}USDT"] = (100.0, 86.0 - index)

    context = None
    for symbol in symbols:
        prev_close, latest_close = prices[symbol]
        seed_symbol(store, symbol, 1_000, prev_close)
        context = accumulator.on_closed_candle(symbol, make_candle(2_000, latest_close))

    assert context is not None
    scorer = SignalContextScorer(context_weight=1.2, risk_weight=0.6)
    adjusted_score, context_score = scorer.evaluate_adjusted_score(
        symbol="ALT0USDT",
        direction="LONG",
        local_score=1.0,
        market_context=context,
        context_model=RuleBasedMarketContextModel(),
    )

    assert context_score.followthrough_score < 0
    assert adjusted_score < 1.0


def test_signal_candidate_can_be_rescored() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")

    context = None
    for index, symbol in enumerate(
        ["BTCUSDT"] + [f"ALT{idx}USDT" for idx in range(39)]
    ):
        seed_symbol(store, symbol, 1_000, 100 + index)
        context = accumulator.on_closed_candle(symbol, make_candle(2_000, 101 + index))

    evaluation = score_signal_candidate_with_context(
        symbol="ALT0USDT",
        direction="LONG",
        score=0.8,
        market_context=context,
        scorer=SignalContextScorer(),
        local_features={"relative_strength_vs_btc": 0.03, "trend_score": 0.02},
    )

    assert evaluation.context_score.confidence == 1.0
    assert evaluation.adjusted_score > 0.8


def test_live_context_detects_market_regime_transition() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")
    symbols = ["BTCUSDT"] + [f"ALT{idx}USDT" for idx in range(39)]

    for index, symbol in enumerate(symbols):
        seed_symbol(store, symbol, 1_000, 100 + index)

    first_context = None
    for index, symbol in enumerate(symbols):
        first_context = accumulator.on_closed_candle(
            symbol, make_candle(2_000, 103 + index)
        )

    assert first_context is not None
    assert first_context.market_regime == "TREND_UP"
    assert first_context.market_regime_transition is None

    second_context = None
    for index, symbol in enumerate(symbols):
        if symbol == "BTCUSDT":
            latest_close = 96.0
        else:
            latest_close = 95.0 - (index * 0.4)
        second_context = accumulator.on_closed_candle(
            symbol,
            make_candle(3_000, latest_close),
        )

    assert second_context is not None
    assert second_context.previous_market_regime == "TREND_UP"
    assert second_context.market_regime_transition is not None
    assert second_context.market_regime_transition_strength > 0.0


def test_transition_flag_stays_true_for_transitional_regime() -> None:
    detector = RegimeTransitionDetector()
    previous_context = make_live_context(
        timestamp=1_000,
        market_regime="TRANSITIONAL",
        regime_is_transitioning=True,
    )
    current_context = make_live_context(timestamp=2_000)

    detector.annotate_context(
        context=current_context,
        previous_context=previous_context,
    )

    assert current_context.market_regime == "TRANSITIONAL"
    assert current_context.market_regime_transition is None
    assert current_context.regime_is_transitioning is True


def test_autotrade_routing_blocks_transitioning_context() -> None:
    context = make_live_context(
        market_regime="TREND_UP",
        regime_is_transitioning=True,
        symbol_features={
            "ALT0USDT": make_symbol_features(micro_regime="RANGE"),
        },
    )

    assert allows_long_autotrade(context=context, symbol="ALT0USDT") is False
    assert supports_grid_trading(context=context, symbol="ALT0USDT") is False

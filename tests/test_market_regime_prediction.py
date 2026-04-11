from pybinbot import SignalsConsumer
from market_regime_prediction.live_market_context_accumulator import (
    LiveMarketContextAccumulator,
)
from market_regime_prediction.context_scoring import RuleBasedMarketContextModel
from market_regime_prediction.market_state_store import MarketStateStore
from market_regime_prediction.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime_prediction.signal_context_scorer import SignalContextScorer


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


def test_live_context_requires_fresh_btc_and_threshold() -> None:
    store = MarketStateStore()
    accumulator = LiveMarketContextAccumulator(store, btc_symbol="BTCUSDT")

    for index in range(39):
        symbol = f"ALT{index}USDT"
        seed_symbol(store, symbol, 1_000, 100 + index)
        assert (
            accumulator.on_closed_candle(symbol, make_candle(2_000, 101 + index))
            is None
        )

    seed_symbol(store, "BTCUSDT", 1_000, 200.0)
    context = accumulator.on_closed_candle("BTCUSDT", make_candle(2_000, 202.0))

    assert context is not None
    assert context.fresh_count == 40
    assert context.confidence == 1.0
    assert context.btc_present is True


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

    candidate = SignalsConsumer(
        symbol="ALT0USDT",
        direction="LONG",
        score=0.8,
        msg="",
        algo="test_market_context",
    )
    evaluation = score_signal_candidate_with_context(
        candidate=candidate,
        market_context=context,
        scorer=SignalContextScorer(),
        local_features={"relative_strength_vs_btc": 0.03, "trend_score": 0.02},
    )

    assert evaluation.context_score.confidence == 1.0
    assert evaluation.adjusted_score > 0.8
    assert candidate.score == evaluation.adjusted_score

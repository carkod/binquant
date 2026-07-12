from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, SymbolModel

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.conservative_spike_hunter import ConservativeSpikeHunter


NOW_MS = 1_700_000_000_000
CANDLE_MS = 15 * 60 * 1000


def make_candles(direction: str = "long", count: int = 60) -> DataFrame:
    sign = 1.0 if direction == "long" else -1.0
    rows: list[dict[str, float]] = []
    close = 100.0
    for index in range(count):
        open_price = close
        if index >= count - 3:
            move = (0.01, 0.015, 0.04)[index - (count - 3)] * sign
        else:
            move = 0.0005 * sign
        close = open_price * (1 + move)
        volume = 2_000.0 if index in {count - 8, count - 5, count - 1} else 100.0
        rows.append(
            {
                "open_time": NOW_MS - (count - index) * CANDLE_MS,
                "close_time": NOW_MS - (count - index) * CANDLE_MS,
                "open": open_price,
                "high": max(open_price, close) * 1.001,
                "low": min(open_price, close) * 0.999,
                "close": close,
                "volume": volume,
                "quote_asset_volume": volume * close,
            }
        )
    return DataFrame(rows)


def make_symbol_features(direction: str = "long", **overrides: Any):
    is_long = direction == "long"
    values = {
        "symbol": "TESTUSDTM",
        "timestamp": NOW_MS - CANDLE_MS,
        "close": 106.0 if is_long else 94.0,
        "return_pct": 0.04 if is_long else -0.04,
        "ema20": 102.0 if is_long else 98.0,
        "ema50": 101.0 if is_long else 99.0,
        "above_ema20": is_long,
        "above_ema50": is_long,
        "trend_score": 0.03 if is_long else -0.03,
        "relative_strength_vs_btc": 0.02 if is_long else -0.02,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "TREND_UP" if is_long else "TREND_DOWN",
        "micro_regime_strength": 0.9,
        "micro_regime_transition": None,
        "micro_regime_transition_strength": 0.0,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_context(direction: str = "long", **overrides: Any) -> LiveMarketContext:
    is_long = direction == "long"
    symbol_features = make_symbol_features(direction)
    context_timestamp = NOW_MS - CANDLE_MS
    values = {
        "timestamp": context_timestamp,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.65 if is_long else 0.35,
        "decliners_ratio": 0.35 if is_long else 0.65,
        "advancers": 65 if is_long else 35,
        "decliners": 35 if is_long else 65,
        "advancers_decliners_ratio": 65 / 35 if is_long else 35 / 65,
        "btc_present": True,
        "fresh_count": 100,
        "total_tracked_symbols": 100,
        "coverage_ratio": 1.0,
        "btc_symbol": "XBTUSDTM",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.02 if is_long else -0.02,
        "average_relative_strength_vs_btc": 0.01 if is_long else -0.01,
        "pct_above_ema20": 0.7 if is_long else 0.3,
        "pct_above_ema50": 0.65 if is_long else 0.35,
        "average_trend_score": 0.05 if is_long else -0.05,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.02 if is_long else -0.02,
        "btc_trend_score": 0.03 if is_long else -0.03,
        "btc_regime_score": 0.2 if is_long else -0.2,
        "long_tailwind": 0.4 if is_long else 0.0,
        "short_tailwind": 0.0 if is_long else 0.4,
        "market_regime": "TREND_UP" if is_long else "TREND_DOWN",
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.8 if is_long else 0.1,
        "short_regime_score": 0.1 if is_long else 0.8,
        "range_regime_score": 0.1,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "regime_stable_since": context_timestamp - 60 * 60 * 1000,
        "symbol_features": {"TESTUSDTM": symbol_features},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_evaluator(
    direction: str = "long",
    *,
    market_type: MarketType = MarketType.FUTURES,
    context: LiveMarketContext | None = None,
    candles: DataFrame | None = None,
) -> SimpleNamespace:
    is_long = direction == "long"
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDTM",
        exchange=ExchangeId.KUCOIN,
        market_type=market_type,
        current_symbol_data=SymbolModel(
            id="TESTUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=6,
        ),
        price_precision=6,
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        market_breadth_data={
            "timestamp": [NOW_MS - CANDLE_MS, NOW_MS - 2 * CANDLE_MS],
            "market_breadth_ma": [0.52, 0.50] if is_long else [0.48, 0.50],
        },
        strategy_cooldowns={},
        latest_market_context=(
            context if context is not None else make_context(direction)
        ),
        df_15m=candles if candles is not None else make_candles(direction),
        dispatch_signal_record=Mock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["long", "short"])
async def test_emits_one_high_confidence_shadow_signal_per_candle(direction: str):
    evaluator = make_evaluator(direction)
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)
    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    call = evaluator.dispatch_signal_record.call_args
    assert call is not None
    value = call.kwargs["value"]
    indicators = call.kwargs["indicators"]
    assert value.autotrade is False
    assert value.score == 1.0
    assert value.bot_params.name == "conservative_spike_hunter"
    assert value.bot_params.market_type == MarketType.FUTURES
    assert value.bot_params.position == direction
    assert all(indicators["components"].values())
    assert indicators["confidence_threshold"] == 0.95


@pytest.mark.asyncio
async def test_spot_market_never_emits():
    evaluator = make_evaluator(market_type=MarketType.SPOT)
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_not_called()
    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_incomplete_candle_history_never_emits():
    candles = make_candles(count=60)
    candles["open_time"] = NOW_MS + CANDLE_MS
    evaluator = make_evaluator(candles=candles)
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_insufficient_candle_history_never_emits():
    evaluator = make_evaluator(candles=make_candles(count=59))
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context",
    [
        None,
        make_context(is_provisional=True),
        make_context(confidence=0.89),
        make_context(coverage_ratio=0.89),
        make_context(btc_present=False),
        make_context(regime_is_transitioning=True),
        make_context(regime_stable_since=None),
        make_context(market_stress_score=0.20),
        make_context(
            timestamp=NOW_MS - 3 * CANDLE_MS,
            regime_stable_since=NOW_MS - 7 * CANDLE_MS,
        ),
        make_context(
            symbol_features={
                "TESTUSDTM": make_symbol_features("long", micro_regime="TREND_DOWN")
            }
        ),
        make_context(symbol_features={}),
    ],
)
async def test_context_vetoes_never_emit(context: LiveMarketContext | None):
    evaluator = make_evaluator(context=context)
    if context is None:
        evaluator.latest_market_context = None
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_missing_timestamped_breadth_momentum_never_emits():
    evaluator = make_evaluator()
    evaluator.market_breadth_data = {"market_breadth_ma": [0.50, 0.52]}
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("market_breadth_data", [None, {}])
async def test_unavailable_market_breadth_skips_before_candle_processing(
    market_breadth_data,
    monkeypatch,
):
    evaluator = make_evaluator()
    evaluator.market_breadth_data = market_breadth_data
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))
    closed_history_mock = Mock()
    monkeypatch.setattr(strategy, "_validated_closed_history", closed_history_mock)

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    closed_history_mock.assert_not_called()
    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_one_missing_five_percent_component_emits_at_threshold():
    weak_strength = make_symbol_features(micro_regime_strength=0.79)
    context = make_context(symbol_features={"TESTUSDTM": weak_strength})
    evaluator = make_evaluator(context=context)
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    call = evaluator.dispatch_signal_record.call_args
    assert call is not None
    assert call.kwargs["value"].score == 0.95
    assert call.kwargs["indicators"]["components"]["micro_regime_strength"] is False


@pytest.mark.asyncio
async def test_one_missing_six_percent_component_does_not_emit():
    evaluator = make_evaluator()
    evaluator.market_breadth_data["market_breadth_ma"] = [0.509, 0.50]
    strategy = ConservativeSpikeHunter(cast(Any, evaluator))

    await strategy.signal(100.0, 110.0, 100.0, 90.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.parametrize(
    ("missing_component", "expected_score", "qualifies"),
    [
        *[
            (name, 0.90, False)
            for name in (
                "volume_cluster",
                "price_break",
                "cumulative_move",
                "acceleration",
                "candle_streak",
            )
        ],
        *[
            (name, 0.94, False)
            for name in (
                "breadth_momentum",
                "regime_score",
                "tailwind",
                "btc_alignment",
                "breadth_participation",
            )
        ],
        *[
            (name, 0.95, True)
            for name in (
                "micro_regime_strength",
                "ema20_alignment",
                "ema50_alignment",
                "symbol_leadership",
            )
        ],
    ],
)
def test_confidence_boundary_for_every_component(
    missing_component: str,
    expected_score: float,
    qualifies: bool,
):
    components = dict.fromkeys(ConservativeSpikeHunter.COMPONENT_WEIGHTS, True)
    components[missing_component] = False

    score = ConservativeSpikeHunter._confidence_score(components)

    assert score == expected_score
    assert (score >= ConservativeSpikeHunter.EMIT_THRESHOLD) is qualifies

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, SymbolModel

from strategies.relative_strength_reversal_range import RelativeStrengthReversalRange
from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    """Default: a relative-strength leader holding up while BTC sells off."""
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.02,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.01,
        "relative_strength_vs_btc": 0.08,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "RANGE",
        "micro_regime_strength": 0.5,
        "micro_regime_transition": None,
        "micro_regime_transition_strength": 0.0,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    """Default: RANGE market with broad sell-off > 2%."""
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.25,
        "advancers_ratio": 0.25,
        "decliners_ratio": 0.75,
        "advancers": 12,
        "decliners": 38,
        "advancers_decliners_ratio": 12 / 38,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": -0.025,
        "average_relative_strength_vs_btc": -0.01,
        "pct_above_ema20": 0.25,
        "pct_above_ema50": 0.35,
        "average_trend_score": -0.03,
        "average_atr_pct": 0.03,
        "average_bb_width": 0.06,
        "btc_return": -0.02,
        "btc_trend_score": -0.02,
        "btc_regime_score": -0.15,
        "long_tailwind": -0.2,
        "short_tailwind": 0.3,
        "market_regime": "RANGE",
        "previous_market_regime": "RANGE",
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.15,
        "short_regime_score": 0.35,
        "range_regime_score": 0.50,
        "stress_regime_score": 0.25,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_df(*, latest_volume: float = 500.0, length: int = 120) -> DataFrame:
    """
    Build a df_15m with enough rows for the 96-bar volume percentile window.
    Background volumes are 100; the most recent bar overrides to `latest_volume`
    so tests can dial the volume gate above or below the 20th percentile.
    """
    rows = []
    for _ in range(length - 1):
        rows.append(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 100.0,
                "quote_asset_volume": 10_000.0,
            }
        )
    rows.append(
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.8,
            "volume": latest_volume,
            "quote_asset_volume": latest_volume * 100.0,
        }
    )
    return DataFrame(rows)


def make_evaluator(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        market_type=MarketType.SPOT,
        df_15m=df,
        dispatch_signal_record=Mock(),
        binbot_api=MagicMock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        latest_market_context=latest_market_context,
        current_symbol_data=SymbolModel(
            id="TESTUSDT",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=8,
            qty_precision=8,
        ),
        price_precision=8,
        qty_precision=8,
        exchange=ExchangeId.KUCOIN,
    )


@pytest.mark.asyncio
async def test_emits_long_when_range_market_sells_off_and_symbol_leads():
    df = make_df(latest_volume=500.0)
    context = make_market_context()
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_not_awaited()

    telegram_msg = evaluator.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Autotrade route: range_rs_reversal" in telegram_msg
    assert "Autotrade is disabled" in telegram_msg
    assert "LONG ENTRY" in telegram_msg

    signal_value = evaluator.dispatch_signal_record.call_args.kwargs["value"]
    assert signal_value.direction == "LONG"
    assert signal_value.autotrade is False
    assert signal_value.bot_params.position == "long"


@pytest.mark.asyncio
async def test_no_signal_when_market_regime_not_range():
    df = make_df(latest_volume=500.0)
    context = make_market_context(market_regime="TREND_DOWN")
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_market_sell_off_too_shallow():
    """avg_return must be < -2%; -1.5% is not weak enough."""
    df = make_df(latest_volume=500.0)
    context = make_market_context(average_return=-0.015)
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_relative_strength_below_threshold():
    """rs vs BTC must be > +5%; +3% is not enough divergence."""
    df = make_df(latest_volume=500.0)
    weak_leader = make_symbol_features(relative_strength_vs_btc=0.03)
    context = make_market_context(symbol_features={"TESTUSDT": weak_leader})
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_volume_below_20th_percentile():
    """
    Background volume is 100, latest is 50 — clearly below the 20th
    percentile floor. The dead-tape guard must reject this.
    """
    df = make_df(latest_volume=50.0)
    context = make_market_context()
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_df_too_short_for_volume_window():
    """Fewer than 96 candles → can't compute a stable percentile; bail."""
    df = make_df(latest_volume=500.0, length=50)
    context = make_market_context()
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_context_missing():
    df = make_df(latest_volume=500.0)
    evaluator = make_evaluator(df, latest_market_context=None)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_symbol_features_missing():
    df = make_df(latest_volume=500.0)
    context = make_market_context(symbol_features={})
    evaluator = make_evaluator(df, context)
    algo = RelativeStrengthReversalRange(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, SymbolModel

from strategies.range_failed_breakout_fade import RangeFailedBreakoutFade
from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.01,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.01,
        "relative_strength_vs_btc": 0.03,
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
    """Default: RANGE market, broad sell-off, symbol outperforming — the
    canonical fade-the-failed-breakout setup."""
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.2,
        "advancers_ratio": 0.30,
        "decliners_ratio": 0.70,
        "advancers": 15,
        "decliners": 35,
        "advancers_decliners_ratio": 15 / 35,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": -0.015,
        "average_relative_strength_vs_btc": -0.005,
        "pct_above_ema20": 0.30,
        "pct_above_ema50": 0.40,
        "average_trend_score": -0.02,
        "average_atr_pct": 0.025,
        "average_bb_width": 0.05,
        "btc_return": -0.01,
        "btc_trend_score": -0.01,
        "btc_regime_score": -0.05,
        "long_tailwind": -0.1,
        "short_tailwind": 0.2,
        "market_regime": "RANGE",
        "previous_market_regime": "RANGE",
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.2,
        "short_regime_score": 0.3,
        "range_regime_score": 0.55,
        "stress_regime_score": 0.2,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_evaluator(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None,
    sh3_signal: dict[str, Any] | None,
) -> SimpleNamespace:
    sh3_stub = SimpleNamespace(latest_signal=Mock(return_value=sh3_signal))
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        market_type=MarketType.FUTURES,
        df_15m=df,
        sh3=sh3_stub,
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


def make_last_spike(
    *,
    upward: bool = True,
    volume_cluster: bool = True,
    cumulative: bool = False,
    accel: bool = False,
    price_break: bool = False,
) -> dict[str, Any]:
    return {
        "timestamp": "2026-05-14 00:00:00",
        "close": 100.8,
        "label": 1,
        "label_pre": 1,
        "label_short": 0,
        "label_short_pre": 0,
        "early_proba_aug_flag": 0,
        "volume_cluster_flag": volume_cluster,
        "price_break_flag": price_break,
        "cumulative_price_break_flag": cumulative,
        "accel_spike_flag": accel,
        "cumulative_price_break_short_flag": False,
        "accel_spike_short_flag": False,
        "signal_type": "FinalSpike",
        "volume": 120.0,
        "quote_asset_volume": 12_000.0,
        "upward": upward,
        "downward": False,
    }


def make_df() -> DataFrame:
    return DataFrame(
        [
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.8,
                "volume": 120.0,
                "quote_asset_volume": 12_000.0,
            }
        ]
    )


@pytest.mark.asyncio
async def test_emits_short_when_range_market_failing_and_symbol_outperforming():
    df = make_df()
    context = make_market_context()
    evaluator = make_evaluator(df, context, make_last_spike())
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()

    telegram_msg = evaluator.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Autotrade route: range_failed_breakout_fade" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg
    assert "SHORT ENTRY" in telegram_msg

    signal_value = evaluator.dispatch_signal_record.call_args.kwargs["value"]
    assert signal_value.direction == "SHORT"
    assert signal_value.autotrade is True
    assert signal_value.bot_params.position == "short"


@pytest.mark.asyncio
async def test_no_signal_when_no_breakout_detected():
    df = make_df()
    context = make_market_context()
    evaluator = make_evaluator(df, context, sh3_signal=None)
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()
    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_spike_not_upward():
    df = make_df()
    context = make_market_context()
    evaluator = make_evaluator(df, context, make_last_spike(upward=False))
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_all_long_flags_off():
    df = make_df()
    context = make_market_context()
    spike = make_last_spike(
        volume_cluster=False,
        cumulative=False,
        accel=False,
        price_break=False,
    )
    evaluator = make_evaluator(df, context, spike)
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_emits_short_on_pure_price_break_flag():
    df = make_df()
    context = make_market_context()
    spike = make_last_spike(
        volume_cluster=False,
        cumulative=False,
        accel=False,
        price_break=True,
    )
    evaluator = make_evaluator(df, context, spike)
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_signal_when_market_regime_not_range():
    df = make_df()
    context = make_market_context(market_regime="TREND_DOWN")
    evaluator = make_evaluator(df, context, make_last_spike())
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_market_not_failing_to_rally():
    """avg_return >= -0.5% should fail the gate even in RANGE."""
    df = make_df()
    context = make_market_context(average_return=-0.005)
    evaluator = make_evaluator(df, context, make_last_spike())
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_symbol_underperforming_btc():
    df = make_df()
    weak_symbol = make_symbol_features(relative_strength_vs_btc=-0.02)
    context = make_market_context(symbol_features={"TESTUSDT": weak_symbol})
    evaluator = make_evaluator(df, context, make_last_spike())
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_context_missing():
    df = make_df()
    evaluator = make_evaluator(
        df, latest_market_context=None, sh3_signal=make_last_spike()
    )
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_symbol_features_missing_from_context():
    """RANGE + failing market but no symbol entry: gate must still reject."""
    df = make_df()
    context = make_market_context(symbol_features={})
    evaluator = make_evaluator(df, context, make_last_spike())
    algo = RangeFailedBreakoutFade(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=102.0,
        bb_low=99.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType

from strategies.spike_hunter_v3_kucoin import SpikeHunterV3KuCoin
from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.02,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.03,
        "relative_strength_vs_btc": 0.02,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "TREND_UP",
        "micro_regime_strength": 0.82,
        "micro_regime_transition": "ENTERED_TREND_UP",
        "micro_regime_transition_strength": 0.4,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.64,
        "decliners_ratio": 0.36,
        "advancers": 32,
        "decliners": 18,
        "advancers_decliners_ratio": 32 / 18,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.013,
        "average_relative_strength_vs_btc": 0.01,
        "pct_above_ema20": 0.68,
        "pct_above_ema50": 0.64,
        "average_trend_score": 0.05,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.01,
        "btc_trend_score": 0.03,
        "btc_regime_score": 0.16,
        "long_tailwind": 0.36,
        "short_tailwind": 0.04,
        "market_regime": "TREND_UP",
        "previous_market_regime": None,
        "market_regime_transition": "ENTERED_TREND_UP",
        "market_regime_transition_strength": 0.45,
        "long_regime_score": 0.71,
        "short_regime_score": 0.18,
        "range_regime_score": 0.24,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_context(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        market_type=MarketType.SPOT,
        df_15m=df,
        binbot_api=MagicMock(),
        telegram_consumer=SimpleNamespace(send_signal=AsyncMock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        latest_market_context=latest_market_context,
        _breadth_cross_tolerance=0.05,
        _autotrade_stress_threshold=0.35,
        current_symbol_data={"base_asset": "TEST", "quote_asset": "USDT"},
        price_precision=8,
        qty_precision=8,
        exchange=ExchangeId.KUCOIN,
    )


def make_algo(
    latest_market_context: LiveMarketContext | None,
) -> tuple[SpikeHunterV3KuCoin, DataFrame]:
    df = DataFrame(
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
    algo = SpikeHunterV3KuCoin(cast(Any, make_context(df, latest_market_context)))
    return algo, df


def make_last_spike(
    *,
    upward: bool = True,
    downward: bool = False,
) -> dict[str, Any]:
    return {
        "timestamp": "2026-04-15 00:00:00",
        "close": 100.8,
        "label": 1,
        "label_pre": 1,
        "early_proba_aug_flag": 0,
        "volume_cluster_flag": True,
        "price_break_flag": False,
        "cumulative_price_break_flag": True,
        "accel_spike_flag": False,
        "signal_type": "FinalSpike",
        "volume": 120.0,
        "quote_asset_volume": 12_000.0,
        "upward": upward,
        "downward": downward,
    }


@pytest.mark.asyncio
async def test_signal_emits_in_trend_up_market(monkeypatch):
    context = make_market_context()
    algo, df = make_algo(context)
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_awaited_once()
    process_mock.assert_awaited_once()
    telegram_await_args = send_signal_mock.await_args
    process_await_args = process_mock.await_args

    assert telegram_await_args is not None
    assert process_await_args is not None

    telegram_msg = telegram_await_args.args[0]
    signal_value = process_await_args.args[0]

    assert "Autotrade route: market_trend_up_symbol_trend_up" in telegram_msg
    assert signal_value.autotrade is True
    assert signal_value.bot_strategy == "long"


@pytest.mark.asyncio
async def test_signal_skips_in_range_market(monkeypatch):
    context = make_market_context(market_regime="RANGE")
    algo, df = make_algo(context)
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_awaited()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_emits_for_bullish_transitional_market(monkeypatch):
    transitional_symbol = make_symbol_features(
        micro_regime="TRANSITIONAL",
        micro_regime_transition="ENTERED_TRANSITIONAL",
        trend_score=0.03,
        above_ema20=True,
        relative_strength_vs_btc=0.03,
    )
    context = make_market_context(
        market_regime="TRANSITIONAL",
        market_regime_transition="LOST_REGIME_EDGE",
        long_tailwind=0.29,
        long_regime_score=0.6,
        short_regime_score=0.19,
        range_regime_score=0.38,
        stress_regime_score=0.1,
        symbol_features={"TESTUSDT": transitional_symbol},
    )
    algo, df = make_algo(context)
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_awaited_once()
    process_mock.assert_awaited_once()
    telegram_await_args = send_signal_mock.await_args

    assert telegram_await_args is not None
    assert (
        "Autotrade route: market_transitional_bullish_symbol_transitional_bullish"
        in (telegram_await_args.args[0])
    )


@pytest.mark.asyncio
async def test_signal_skips_downward_spike_even_in_bullish_market(monkeypatch):
    context = make_market_context()
    algo, df = make_algo(context)
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    monkeypatch.setattr(
        algo,
        "latest_signal",
        lambda: make_last_spike(upward=False, downward=True),
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_awaited()
    process_mock.assert_not_awaited()

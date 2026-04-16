from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType

from strategies.liquidation_sweep_pump import LiquidationSweepPump
from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.015,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.02,
        "relative_strength_vs_btc": 0.01,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "TREND_UP",
        "micro_regime_strength": 0.8,
        "micro_regime_transition": "ENTERED_TREND_UP",
        "micro_regime_transition_strength": 0.4,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.62,
        "decliners_ratio": 0.38,
        "advancers": 31,
        "decliners": 19,
        "advancers_decliners_ratio": 31 / 19,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.012,
        "average_relative_strength_vs_btc": 0.009,
        "pct_above_ema20": 0.68,
        "pct_above_ema50": 0.64,
        "average_trend_score": 0.05,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.01,
        "btc_trend_score": 0.03,
        "btc_regime_score": 0.15,
        "long_tailwind": 0.35,
        "short_tailwind": 0.05,
        "market_regime": "TREND_UP",
        "previous_market_regime": None,
        "market_regime_transition": "ENTERED_TREND_UP",
        "market_regime_transition_strength": 0.45,
        "long_regime_score": 0.7,
        "short_regime_score": 0.18,
        "range_regime_score": 0.25,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_price_df() -> DataFrame:
    rows = []
    price = 100.0
    for idx in range(50):
        price *= 1.002
        rows.append(
            {
                "open": price * 0.998,
                "high": price * 1.004,
                "low": price * 0.996,
                "close": price,
                "volume": 100.0 + idx,
            }
        )
    return DataFrame(rows)


def make_pump_ready_df(df: DataFrame) -> DataFrame:
    enriched = df.copy()
    enriched["pump_score"] = 1.0
    enriched["pump_score_smooth"] = 1.0
    enriched.loc[enriched.index[-1], "pump_score"] = 12.0
    enriched.loc[enriched.index[-1], "pump_score_smooth"] = 10.0
    return enriched


def make_context(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        binbot_api=MagicMock(),
        telegram_consumer=SimpleNamespace(send_signal=AsyncMock()),
        market_type=MarketType.SPOT,
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        current_symbol_data={"base_asset": "TEST"},
        price_precision=8,
        qty_precision=8,
        oi_data=1.05,
        latest_market_context=latest_market_context,
        df_15m=df,
        df_btc=df,
    )


def make_algo(
    latest_market_context: LiveMarketContext | None,
) -> tuple[LiquidationSweepPump, DataFrame]:
    df = make_price_df()
    algo = LiquidationSweepPump(cast(Any, make_context(df, latest_market_context)))
    return algo, df


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

    monkeypatch.setattr(algo, "compute_pump_score", lambda _: make_pump_ready_df(df))
    monkeypatch.setattr(
        "strategies.liquidation_sweep_pump.build_links_msg",
        lambda env, exchange, market_type, symbol: ("https://exchange", "https://bot"),
    )

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

    assert "Route: market_trend_up_symbol_trend_up" in telegram_msg
    assert signal_value.autotrade is True


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

    monkeypatch.setattr(algo, "compute_pump_score", lambda _: make_pump_ready_df(df))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_awaited()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_in_trend_down_market(monkeypatch):
    context = make_market_context(market_regime="TREND_DOWN")
    algo, df = make_algo(context)
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    monkeypatch.setattr(algo, "compute_pump_score", lambda _: make_pump_ready_df(df))

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
        relative_strength_vs_btc=0.02,
    )
    context = make_market_context(
        market_regime="TRANSITIONAL",
        market_regime_transition="LOST_REGIME_EDGE",
        long_tailwind=0.28,
        long_regime_score=0.61,
        short_regime_score=0.2,
        range_regime_score=0.4,
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

    monkeypatch.setattr(algo, "compute_pump_score", lambda _: make_pump_ready_df(df))
    monkeypatch.setattr(
        "strategies.liquidation_sweep_pump.build_links_msg",
        lambda env, exchange, market_type, symbol: ("https://exchange", "https://bot"),
    )

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
    telegram_msg = telegram_await_args.args[0]

    assert "Route: market_transitional_bullish_symbol_transitional_bullish" in (
        telegram_msg
    )

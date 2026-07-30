from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketBreadthSeries, MarketType, SymbolModel

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.ride_market_breadth import RideMarketBreadth


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDTM",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": -0.01,
        "ema20": 101.0,
        "ema50": 102.0,
        "above_ema20": False,
        "above_ema50": False,
        "trend_score": -0.03,
        "relative_strength_vs_btc": -0.02,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "TREND_DOWN",
        "micro_regime_strength": 0.82,
        "micro_regime_transition": "BREAKDOWN",
        "micro_regime_transition_strength": 0.4,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.55,
        "decliners_ratio": 0.45,
        "advancers": 28,
        "decliners": 22,
        "advancers_decliners_ratio": 28 / 22,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.003,
        "average_relative_strength_vs_btc": 0.0,
        "pct_above_ema20": 0.54,
        "pct_above_ema50": 0.50,
        "average_trend_score": 0.0,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.0,
        "btc_trend_score": 0.0,
        "btc_regime_score": 0.0,
        "long_tailwind": 0.1,
        "short_tailwind": 0.15,
        "market_regime": "TRANSITIONAL",
        "previous_market_regime": None,
        "market_regime_transition": "LOST_REGIME_EDGE",
        "market_regime_transition_strength": 0.45,
        "long_regime_score": 0.35,
        "short_regime_score": 0.42,
        "range_regime_score": 0.24,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": True,
        "symbol_features": {"TESTUSDTM": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_market_breadth_data(
    values: list[float],
    *,
    ma_values: list[float] | None = None,
) -> MarketBreadthSeries:
    market_breadth_ma = ma_values if ma_values is not None else values
    return MarketBreadthSeries(
        timestamp=[
            "2026-07-04 00:00:00",
            "2026-07-04 00:15:00",
            "2026-07-04 00:30:00",
            "2026-07-04 00:45:00",
        ],
        advancers=[25, 24, 23, 28],
        decliners=[25, 26, 27, 22],
        market_breadth=values,
        market_breadth_ma=market_breadth_ma,
        avg_gain=[0.01, 0.01, 0.01, 0.02],
        avg_loss=[-0.01, -0.01, -0.01, -0.01],
        total_volume=[1000, 1000, 1000, 1200],
        strength_index=[0.1, 0.1, 0.1, 0.2],
    )


def make_candles(*, bearish_last: bool) -> DataFrame:
    rows: list[dict[str, float | int]] = []
    for index in range(25):
        open_price = 100.0 + index * 0.1
        close_price = open_price + 0.2
        if index == 24 and bearish_last:
            close_price = open_price - 0.35
        rows.append(
            {
                "open_time": 1_700_000_000_000 + index * 900_000,
                "open": open_price,
                "high": max(open_price, close_price) + 0.4,
                "low": min(open_price, close_price) - 0.4,
                "close": close_price,
                "volume": 120.0 if index == 24 else 100.0,
                "quote_asset_volume": 12_000.0,
            }
        )
    return DataFrame(rows)


def make_context(
    *,
    latest_market_context: LiveMarketContext,
    market_breadth_data: MarketBreadthSeries,
    df_15m: DataFrame,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDTM",
        market_type=MarketType.FUTURES,
        df_15m=df_15m,
        dispatch_signal_record=Mock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        latest_market_context=latest_market_context,
        market_breadth_data=market_breadth_data,
        current_symbol_data=SymbolModel(
            id="TESTUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=8,
        ),
        price_precision=8,
        exchange=ExchangeId.KUCOIN,
        strategy_cooldowns={},
    )


@pytest.mark.asyncio
async def test_signal_opens_short_when_positive_breadth_breaks_downtrend_with_bearish_micro_regime(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "staging")
    context = make_market_context()
    df = make_candles(bearish_last=True)
    algo = RideMarketBreadth(
        cast(
            Any,
            make_context(
                latest_market_context=context,
                market_breadth_data=make_market_breadth_data([0.18, 0.17, 0.16, 0.17]),
                df_15m=df,
            ),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_msg = send_signal_mock.call_args.args[0]
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]

    assert "Breadth setup: positive_breadth_downtrend_break_pop" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg
    assert signal_value.autotrade is True
    assert signal_value.bot_params.position == "short"


@pytest.mark.asyncio
async def test_signal_records_shadow_long_outside_staging_when_negative_breadth_projects_zero_cross_with_recovery_micro_regime(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "production")
    context = make_market_context(
        long_regime_score=0.42,
        short_regime_score=0.32,
        symbol_features={
            "TESTUSDTM": make_symbol_features(
                return_pct=0.01,
                ema20=99.0,
                ema50=98.0,
                above_ema20=True,
                above_ema50=True,
                trend_score=0.03,
                relative_strength_vs_btc=0.02,
                micro_regime="TRANSITIONAL",
                micro_regime_transition="RECOVERY",
            )
        },
    )
    df = make_candles(bearish_last=False)
    algo = RideMarketBreadth(
        cast(
            Any,
            make_context(
                latest_market_context=context,
                market_breadth_data=make_market_breadth_data(
                    [-0.30, -0.29, -0.27, -0.22]
                ),
                df_15m=df,
            ),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_msg = send_signal_mock.call_args.args[0]
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]

    assert "Breadth setup: negative_breadth_recovery_to_zero" in telegram_msg
    assert "Autotrade is disabled outside staging" in telegram_msg
    assert signal_value.autotrade is False
    assert signal_value.bot_params.position == "long"


@pytest.mark.asyncio
async def test_signal_uses_raw_breadth_when_ma_does_not_break(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "staging")
    context = make_market_context(
        long_regime_score=0.42,
        short_regime_score=0.32,
        symbol_features={
            "TESTUSDTM": make_symbol_features(
                return_pct=0.01,
                ema20=99.0,
                ema50=98.0,
                above_ema20=True,
                above_ema50=True,
                trend_score=0.03,
                relative_strength_vs_btc=0.02,
                micro_regime="TRANSITIONAL",
                micro_regime_transition="RECOVERY",
            )
        },
    )
    df = make_candles(bearish_last=False)
    algo = RideMarketBreadth(
        cast(
            Any,
            make_context(
                latest_market_context=context,
                market_breadth_data=make_market_breadth_data(
                    [-0.30, -0.295, -0.275, -0.25],
                    ma_values=[-0.30, -0.295, -0.29, -0.285],
                ),
                df_15m=df,
            ),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]
    signal_record = record_mock.call_args.kwargs["value"]
    indicators = record_mock.call_args.kwargs["indicators"]

    assert signal_value is signal_record
    assert signal_value.autotrade is True
    assert signal_value.bot_params.position == "long"
    assert indicators["breadth_source"] == "market_breadth"


@pytest.mark.asyncio
async def test_signal_skips_when_micro_regime_does_not_confirm_breadth_route(
    monkeypatch,
):
    context = make_market_context(
        symbol_features={
            "TESTUSDTM": make_symbol_features(
                trend_score=0.03,
                relative_strength_vs_btc=0.02,
                micro_regime="TREND_UP",
                micro_regime_transition="BREAKOUT_UP",
            )
        },
    )
    df = make_candles(bearish_last=True)
    algo = RideMarketBreadth(
        cast(
            Any,
            make_context(
                latest_market_context=context,
                market_breadth_data=make_market_breadth_data([0.18, 0.17, 0.16, 0.17]),
                df_15m=df,
            ),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()

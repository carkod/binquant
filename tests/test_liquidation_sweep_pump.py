from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, Position, SymbolModel
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


def make_btc_df(last_change: float) -> DataFrame:
    df = make_price_df()
    prev_close = float(df.close.iloc[-2])
    df.loc[df.index[-1], "close"] = prev_close * (1 + last_change)
    return df


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
    market_breadth_data: dict[str, list[float]] | None = None,
    df_btc_15m: DataFrame | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        dispatch_signal_record=Mock(),
        binbot_api=MagicMock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        market_type=MarketType.SPOT,
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
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
        oi_data=1.05,
        market_breadth_data=market_breadth_data or {"adp": [0.0, 0.0]},
        latest_market_context=latest_market_context,
        df_15m=df,
        df_btc_15m=df_btc_15m if df_btc_15m is not None else df,
    )


def make_algo(
    latest_market_context: LiveMarketContext | None,
    market_breadth_data: dict[str, list[float]] | None = None,
    btc_last_change: float = 0.0,
) -> tuple[LiquidationSweepPump, DataFrame]:
    df = make_price_df()
    algo = LiquidationSweepPump(
        cast(
            Any,
            make_context(
                df,
                latest_market_context,
                market_breadth_data=market_breadth_data,
                df_btc_15m=make_btc_df(btc_last_change),
            ),
        )
    )
    return algo, df


@pytest.mark.asyncio
async def test_signal_emits_short_when_hot_breadth_fades_with_stalled_btc_and_weak_symbol(
    monkeypatch,
):
    weak_symbol = make_symbol_features(
        micro_regime="TRANSITIONAL",
        trend_score=-0.01,
        above_ema20=False,
        relative_strength_vs_btc=-0.02,
    )
    context = make_market_context(
        advancers_ratio=0.66,
        decliners_ratio=0.34,
        symbol_features={"TESTUSDT": weak_symbol},
    )
    algo, df = make_algo(
        context,
        market_breadth_data={"adp": [0.18, 0.36, 0.32]},
        btc_last_change=0.001,
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
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

    send_signal_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_await_args = send_signal_mock.call_args
    process_await_args = process_mock.await_args

    assert telegram_await_args is not None
    assert process_await_args is not None

    telegram_msg = telegram_await_args.args[0]
    signal_value = process_await_args.args[0]

    assert signal_value.bot_params.position == Position.short
    assert signal_value.direction == "SHORT"
    assert signal_value.autotrade is True
    assert "Action: SHORT ENTRY" in telegram_msg
    assert "Autotrade route: breadth_hot_fading_btc_stalled_symbol_weak" in telegram_msg


@pytest.mark.asyncio
async def test_signal_skips_short_when_hot_breadth_is_not_falling(monkeypatch):
    weak_symbol = make_symbol_features(
        micro_regime="TRANSITIONAL",
        trend_score=-0.01,
        above_ema20=False,
        relative_strength_vs_btc=-0.02,
    )
    context = make_market_context(
        advancers_ratio=0.66,
        decliners_ratio=0.34,
        symbol_features={"TESTUSDT": weak_symbol},
    )
    algo, df = make_algo(
        context,
        market_breadth_data={"adp": [0.18, 0.30, 0.32]},
        btc_last_change=0.001,
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
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

    send_signal_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_short_when_symbol_followthrough_is_not_weak(monkeypatch):
    strong_symbol = make_symbol_features(
        micro_regime="TREND_UP",
        trend_score=0.03,
        above_ema20=True,
        relative_strength_vs_btc=0.02,
    )
    context = make_market_context(
        advancers_ratio=0.66,
        decliners_ratio=0.34,
        symbol_features={"TESTUSDT": strong_symbol},
    )
    algo, df = make_algo(
        context,
        market_breadth_data={"adp": [0.18, 0.36, 0.32]},
        btc_last_change=0.001,
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
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

    send_signal_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_emits_long_when_washed_out_breadth_recovers_with_btc(
    monkeypatch,
):
    context = make_market_context(
        advancers_ratio=0.29,
        decliners_ratio=0.71,
    )
    algo, df = make_algo(
        context,
        market_breadth_data={"adp": [-0.52, -0.46, -0.42]},
        btc_last_change=0.003,
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
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

    send_signal_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_await_args = send_signal_mock.call_args
    process_await_args = process_mock.await_args

    assert telegram_await_args is not None
    assert process_await_args is not None

    telegram_msg = telegram_await_args.args[0]
    signal_value = process_await_args.args[0]

    assert signal_value.bot_params.position == Position.long
    assert signal_value.direction == "LONG"
    assert signal_value.autotrade is True
    assert "Action: LONG ENTRY" in telegram_msg
    assert "Autotrade route: breadth_washed_out_recovering_btc_up" in telegram_msg


@pytest.mark.asyncio
async def test_signal_skips_long_when_btc_is_not_increasing(monkeypatch):
    context = make_market_context(
        advancers_ratio=0.29,
        decliners_ratio=0.71,
    )
    algo, df = make_algo(
        context,
        market_breadth_data={"adp": [-0.52, -0.46, -0.42]},
        btc_last_change=0.0,
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
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

    send_signal_mock.assert_not_called()
    process_mock.assert_not_awaited()

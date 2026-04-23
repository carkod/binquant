from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, Position

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.top_gainers_reversal_drop import TopGainersReversalDrop


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 1.085,
        "return_pct": -0.02,
        "ema20": 1.09,
        "ema50": 1.08,
        "above_ema20": False,
        "above_ema50": True,
        "trend_score": -0.01,
        "relative_strength_vs_btc": -0.01,
        "atr_pct": 0.03,
        "bb_width": 0.05,
        "micro_regime": "TREND_DOWN",
        "micro_regime_strength": 0.8,
        "micro_regime_transition": None,
        "micro_regime_transition_strength": 0.0,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.42,
        "decliners_ratio": 0.58,
        "advancers": 21,
        "decliners": 29,
        "advancers_decliners_ratio": 21 / 29,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "XBTUSDTM",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": -0.003,
        "average_relative_strength_vs_btc": -0.001,
        "pct_above_ema20": 0.42,
        "pct_above_ema50": 0.38,
        "average_trend_score": -0.01,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.05,
        "btc_return": -0.004,
        "btc_trend_score": -0.02,
        "btc_regime_score": -0.15,
        "long_tailwind": 0.12,
        "short_tailwind": 0.3,
        "market_regime": "TREND_DOWN",
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.18,
        "short_regime_score": 0.62,
        "range_regime_score": 0.22,
        "stress_regime_score": 0.12,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_context(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        binbot_api=SimpleNamespace(get_top_gainers=AsyncMock(return_value=[])),
        telegram_consumer=SimpleNamespace(send_signal=AsyncMock()),
        market_type=MarketType.SPOT,
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        current_symbol_data={"base_asset": "TEST"},
        price_precision=8,
        qty_precision=8,
        df_5m=df,
        first_seen_at=0,
        interval=SimpleNamespace(get_ms=lambda: 60_000),
        latest_market_context=latest_market_context,
        _breadth_cross_tolerance=0.05,
        _autotrade_stress_threshold=0.35,
        bot_strategy=Position.long,
    )


def make_algo(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None = None,
) -> TopGainersReversalDrop:
    return TopGainersReversalDrop(cast(Any, make_context(df, latest_market_context)))


def make_reversal_df() -> DataFrame:
    rows = []
    close = 1.0

    for _ in range(24):
        rows.append(
            {
                "open": close,
                "high": close * 1.001,
                "low": close * 0.999,
                "close": close,
                "volume": 1.0,
            }
        )

    rows.extend(
        [
            {
                "open": 1.0,
                "high": 1.03,
                "low": 0.995,
                "close": 1.025,
                "volume": 2.0,
            },
            {
                "open": 1.025,
                "high": 1.07,
                "low": 1.02,
                "close": 1.055,
                "volume": 2.4,
            },
            {
                "open": 1.055,
                "high": 1.11,
                "low": 1.05,
                "close": 1.09,
                "volume": 2.8,
            },
            {
                "open": 1.09,
                "high": 1.15,
                "low": 1.08,
                "close": 1.13,
                "volume": 3.0,
            },
            {
                "open": 1.135,
                "high": 1.165,
                "low": 1.07,
                "close": 1.085,
                "volume": 4.5,
            },
        ]
    )

    return DataFrame(rows)


@pytest.mark.asyncio
async def test_signal_dispatches_for_top_gainer_reversal():
    df = make_reversal_df()
    algo = make_algo(df)
    get_top_gainers_mock = AsyncMock(
        return_value=[
            {"symbol": "TESTUSDT", "priceChangePercent": "15.8"},
            {"symbol": "ABCUSDT", "priceChangePercent": "9.2"},
        ]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    send_signal_mock.assert_awaited_once()
    process_mock.assert_awaited_once()

    await_args = process_mock.await_args
    assert await_args is not None
    value = await_args.args[0]
    assert value.bot_params.name == "top_gainers_reversal_drop"
    assert value.bot_params.pair == "TESTUSDT"
    assert value.bot_params.position == "short"


@pytest.mark.asyncio
async def test_signal_generator_skips_when_symbol_is_not_a_top_gainer():
    df = make_reversal_df()
    algo = make_algo(df)
    get_top_gainers_mock = AsyncMock(
        return_value=[{"symbol": "ABCUSDT", "priceChangePercent": "15.8"}]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    send_signal_mock.assert_not_awaited()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_when_price_change_percent_is_invalid():
    df = make_reversal_df()
    algo = make_algo(df)
    get_top_gainers_mock = AsyncMock(
        return_value=[{"symbol": "TESTUSDT", "priceChangePercent": None}]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    send_signal_mock.assert_not_awaited()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_disables_autotrade_in_range_market():
    df = make_reversal_df()
    algo = make_algo(
        df, latest_market_context=make_market_context(market_regime="RANGE")
    )
    get_top_gainers_mock = AsyncMock(
        return_value=[{"symbol": "TESTUSDT", "priceChangePercent": "15.8"}]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    process_args = process_mock.await_args
    assert process_args is not None
    value = process_args.args[0]
    assert value.autotrade is False
    send_args = send_signal_mock.await_args
    assert send_args is not None
    telegram_msg = send_args.args[0]
    assert "Autotrade route: market_regime_range" in telegram_msg


@pytest.mark.asyncio
async def test_signal_disables_autotrade_in_trend_up_market():
    df = make_reversal_df()
    algo = make_algo(
        df,
        latest_market_context=make_market_context(
            market_regime="TREND_UP",
            long_regime_score=0.62,
            short_regime_score=0.2,
            range_regime_score=0.22,
        ),
    )
    get_top_gainers_mock = AsyncMock(
        return_value=[{"symbol": "TESTUSDT", "priceChangePercent": "15.8"}]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    process_args = process_mock.await_args
    assert process_args is not None
    value = process_args.args[0]
    assert value.autotrade is False
    send_args = send_signal_mock.await_args
    assert send_args is not None
    telegram_msg = send_args.args[0]
    assert "Autotrade route: market_regime_trend_up" in telegram_msg


@pytest.mark.asyncio
async def test_signal_enables_autotrade_in_trend_down_market():
    df = make_reversal_df()
    algo = make_algo(df, latest_market_context=make_market_context())
    get_top_gainers_mock = AsyncMock(
        return_value=[{"symbol": "TESTUSDT", "priceChangePercent": "15.8"}]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    process_args = process_mock.await_args
    assert process_args is not None
    value = process_args.args[0]
    assert value.autotrade is True
    send_args = send_signal_mock.await_args
    assert send_args is not None
    telegram_msg = send_args.args[0]
    assert "Autotrade route: short_autotrade_allowed" in telegram_msg


@pytest.mark.asyncio
async def test_signal_disables_autotrade_when_market_regime_is_unavailable() -> None:
    df = make_reversal_df()
    algo = make_algo(
        df,
        latest_market_context=make_market_context(
            market_regime=None,
            symbol_features={
                "TESTUSDT": make_symbol_features(micro_regime="TREND_DOWN")
            },
        ),
    )
    get_top_gainers_mock = AsyncMock(
        return_value=[{"symbol": "TESTUSDT", "priceChangePercent": "15.8"}]
    )
    send_signal_mock = AsyncMock()
    process_mock = AsyncMock()
    algo.binbot_api = cast(Any, SimpleNamespace(get_top_gainers=get_top_gainers_mock))
    algo.telegram_consumer = cast(Any, SimpleNamespace(send_signal=send_signal_mock))
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=0, bb_mid=0, bb_low=0
    )

    process_args = process_mock.await_args
    assert process_args is not None
    value = process_args.args[0]
    assert value.autotrade is False
    send_args = send_signal_mock.await_args
    assert send_args is not None
    telegram_msg = send_args.args[0]
    assert "Autotrade route: market_regime_unavailable" in telegram_msg

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, Strategy

from algorithms.top_gainers_reversal_drop import TopGainersReversalDrop


def make_context(df: DataFrame) -> SimpleNamespace:
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
        df=df,
        first_seen_at=0,
        interval=SimpleNamespace(get_ms=lambda: 60_000),
        should_autotrade=lambda strategy, requested=True: requested,
        bot_strategy=Strategy.long,
    )


def make_algo(df: DataFrame) -> TopGainersReversalDrop:
    return TopGainersReversalDrop(cast(Any, make_context(df)))


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
    assert value.algo == "top_gainers_reversal_drop"
    assert value.symbol == "TESTUSDT"
    assert value.bot_strategy == "margin_short"


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

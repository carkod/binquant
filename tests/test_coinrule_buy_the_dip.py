from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType
from unittest.mock import AsyncMock, MagicMock

from strategies.coinrule.buy_the_dip import CoinruleBuyTheDip


def make_buy_the_dip_df(
    closes: list[float],
    end_time: datetime | None = None,
) -> DataFrame:
    if end_time is None:
        end_time = datetime(2026, 4, 13, 12, 0, tzinfo=UTC)

    rows = []
    start_time = end_time - timedelta(minutes=15 * (len(closes) - 1))

    for index, close in enumerate(closes):
        candle_time = start_time + timedelta(minutes=15 * index)
        rows.append(
            {
                "open": close * 1.001,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 100.0,
                "close_time": int(candle_time.timestamp() * 1000),
            }
        )

    return DataFrame(rows)


def make_algo(df_15m: DataFrame) -> tuple[CoinruleBuyTheDip, SimpleNamespace]:
    context = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.SPOT,
        telegram_consumer=SimpleNamespace(send_signal=AsyncMock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        latest_market_context=None,
        df_15m=df_15m,
        binbot_api=MagicMock(),
    )
    return CoinruleBuyTheDip(cast(Any, context)), context


@pytest.mark.asyncio
async def test_buy_the_dip_enters_on_valid_six_hour_dip() -> None:
    closes = [100.0] + [99.8] * 23 + [97.0]
    algo, context = make_algo(make_buy_the_dip_df(closes))

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.send_signal.assert_awaited_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()

    telegram_msg = context.telegram_consumer.send_signal.await_args.args[0]
    assert "coinrule_buy_the_dip" in telegram_msg
    assert "Action: LONG ENTRY" in telegram_msg
    assert "6h price change: -3.0%" in telegram_msg


@pytest.mark.asyncio
async def test_buy_the_dip_skips_before_start_time() -> None:
    closes = [100.0] + [99.8] * 23 + [97.0]
    end_time = datetime(2026, 4, 12, 22, 0, tzinfo=UTC)
    algo, context = make_algo(make_buy_the_dip_df(closes, end_time=end_time))

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.send_signal.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_skips_when_dip_is_too_small() -> None:
    closes = [100.0] + [99.9] * 23 + [98.5]
    algo, context = make_algo(make_buy_the_dip_df(closes))

    await algo.signal(
        current_price=98.5,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.send_signal.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_skips_when_dip_is_too_deep() -> None:
    closes = [100.0] + [99.5] * 23 + [95.0]
    algo, context = make_algo(make_buy_the_dip_df(closes))

    await algo.signal(
        current_price=95.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.send_signal.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_can_emit_repeated_signals_without_local_cooldown() -> None:
    closes = [100.0] + [99.8] * 23 + [97.0]
    algo, context = make_algo(make_buy_the_dip_df(closes))

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )
    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    assert context.telegram_consumer.send_signal.await_count == 2
    assert context.at_consumer.process_autotrade_restrictions.await_count == 2

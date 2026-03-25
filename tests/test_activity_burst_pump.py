from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from pybinbot import ExchangeId, MarketType

from algorithms.activity_burst_pump import ActivityBurstPump


def make_context(df: pd.DataFrame) -> SimpleNamespace:
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
        df=df,
        first_seen_at=0,
        interval=SimpleNamespace(get_ms=lambda: 60_000),
    )


def make_algo(df: pd.DataFrame) -> ActivityBurstPump:
    return ActivityBurstPump(cast(Any, make_context(df)))


def make_low_liquidity_df() -> pd.DataFrame:
    rows = []
    close = 1.0

    for idx in range(23):
        rows.append(
            {
                "open": close,
                "high": close * 1.001,
                "low": close * 0.999,
                "close": close,
                "volume": 0.0 if idx < 10 else 1.0,
            }
        )

    rows.append(
        {
            "open": close,
            "high": 1.055,
            "low": 0.998,
            "close": 1.04,
            "volume": 10.0,
        }
    )

    return pd.DataFrame(rows)


def test_compute_indicators_uses_median_baseline():
    df = make_low_liquidity_df()
    algo = make_algo(df)

    indicators = algo.compute_indicators()
    row = indicators.iloc[-1]

    assert row["baseline_volume"] == pytest.approx(1.0)
    assert row["volume_ratio"] == pytest.approx(10.0)
    assert row["price_jump"] == pytest.approx(0.04)
    assert bool(row["vol_spike"]) is True
    assert bool(row["price_jump_flag"]) is True
    assert row["activity_burst_score"] == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_signal_generator_dispatches_on_volume_and_price_burst(monkeypatch):
    df = make_low_liquidity_df()
    algo = make_algo(df)
    handle_mock = AsyncMock()
    algo.signal_collector = cast(Any, SimpleNamespace(handle=handle_mock))

    monkeypatch.setattr(
        "algorithms.activity_burst_pump.build_links_msg",
        lambda env, exchange, market_type, symbol: ("https://exchange", "https://bot"),
    )

    await algo.signal_generator(current_price=float(df.close.iloc[-1]))

    handle_mock.assert_awaited_once()
    await_args = handle_mock.await_args
    assert await_args is not None
    candidate = await_args.kwargs["candidate"]

    assert candidate.algo == "activity_burst_pump"
    assert candidate.symbol == "TESTUSDT"
    assert candidate.score == pytest.approx(0.4)
    assert candidate.volume == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_signal_generator_skips_when_price_jump_is_too_small():
    df = make_low_liquidity_df()
    df.loc[df.index[-1], "close"] = 1.01
    df.loc[df.index[-1], "high"] = 1.012
    algo = make_algo(df)
    handle_mock = AsyncMock()
    algo.signal_collector = cast(Any, SimpleNamespace(handle=handle_mock))

    await algo.signal_generator(current_price=float(df.close.iloc[-1]))

    handle_mock.assert_not_awaited()

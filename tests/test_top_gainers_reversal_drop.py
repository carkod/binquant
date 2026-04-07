from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from pybinbot import ExchangeId, MarketType, Strategy

from algorithms.top_gainers_reversal_drop import TopGainersReversalDrop


def make_context(df: pd.DataFrame) -> SimpleNamespace:
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
    )


def make_algo(df: pd.DataFrame) -> TopGainersReversalDrop:
    return TopGainersReversalDrop(cast(Any, make_context(df)))


def make_reversal_df() -> pd.DataFrame:
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

    return pd.DataFrame(rows)


def test_match_top_gainer_supports_kucoin_style_symbol():
    algo = make_algo(make_reversal_df())

    rank, record, gain_pct = algo._match_top_gainer(
        [
            {"symbol": "ABC-USDT", "priceChangePercent": "9.1"},
            {"symbol": "TEST-USDT", "priceChangePercent": "17.4"},
        ]
    )

    assert rank == 2
    assert record == {"symbol": "TEST-USDT", "priceChangePercent": "17.4"}
    assert gain_pct == pytest.approx(17.4)


@pytest.mark.asyncio
async def test_signal_generator_dispatches_for_top_gainer_reversal(monkeypatch):
    df = make_reversal_df()
    algo = make_algo(df)
    algo.binbot_api = SimpleNamespace(
        get_top_gainers=AsyncMock(
            return_value=[
                {"symbol": "TEST-USDT", "priceChangePercent": "15.8"},
                {"symbol": "ABC-USDT", "priceChangePercent": "9.2"},
            ]
        )
    )
    handle_mock = AsyncMock()
    algo.signal_collector = cast(Any, SimpleNamespace(handle=handle_mock))

    monkeypatch.setattr(
        "algorithms.top_gainers_reversal_drop.build_links_msg",
        lambda env, exchange, market_type, symbol: ("https://exchange", "https://bot"),
    )

    await algo.signal_generator(current_price=float(df.close.iloc[-1]))

    handle_mock.assert_awaited_once()
    await_args = handle_mock.await_args
    assert await_args is not None
    candidate = await_args.kwargs["candidate"]
    assert candidate.algo == "top_gainers_reversal_drop"
    assert candidate.symbol == "TESTUSDT"
    assert candidate.strategy == Strategy.margin_short
    assert candidate.score > 0


@pytest.mark.asyncio
async def test_signal_generator_skips_when_symbol_is_not_a_top_gainer():
    df = make_reversal_df()
    algo = make_algo(df)
    algo.binbot_api = SimpleNamespace(
        get_top_gainers=AsyncMock(
            return_value=[{"symbol": "ABC-USDT", "priceChangePercent": "15.8"}]
        )
    )
    handle_mock = AsyncMock()
    algo.signal_collector = cast(Any, SimpleNamespace(handle=handle_mock))

    await algo.signal_generator(current_price=float(df.close.iloc[-1]))

    handle_mock.assert_not_awaited()

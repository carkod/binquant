from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, MarketDominance, Strategy

from algorithms.coinrule import GridTrading, PriceTracker


def make_context(df: DataFrame) -> SimpleNamespace:
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
        df_1h=df,
        market_breadth_data={"adp": [0, 1, 2]},
        bot_strategy=Strategy.long,
        current_market_dominance=MarketDominance.NEUTRAL,
        market_domination_reversal=False,
        first_seen_at=0,
        interval=SimpleNamespace(get_ms=lambda: 60_000),
        latest_market_context=None,
        _breadth_cross_tolerance=0.05,
        _autotrade_stress_threshold=0.35,
    )


def make_algo(df: DataFrame) -> PriceTracker:
    return PriceTracker(cast(Any, make_context(df)))


def make_grid_algo(df: DataFrame) -> GridTrading:
    return GridTrading(cast(Any, make_context(df)))


def make_ohlcv_df(n: int = 50, oversold: bool = False) -> DataFrame:
    """
    Create a minimal OHLCV dataframe.
    When oversold=True, prices trend sharply downward to produce
    RSI < 30 and negative MACD, with low volume to produce MFI < 20.
    """
    rows = []
    if oversold:
        # Simulate a strong downtrend: each close lower than previous
        price = 100.0
        for i in range(n):
            price *= 0.985  # consistent ~1.5% drop per candle
            rows.append(
                {
                    "open": price * 1.01,
                    "high": price * 1.015,
                    "low": price * 0.985,
                    "close": price,
                    "volume": 0.01,  # very low volume → low MFI
                    "close_time": i * 300_000,
                    "number_of_trades": 10,
                }
            )
    else:
        # Neutral / uptrend – RSI will not be below 30
        price = 100.0
        for i in range(n):
            price *= 1.005
            rows.append(
                {
                    "open": price * 0.99,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "volume": 1000.0,
                    "close_time": i * 300_000,
                    "number_of_trades": 10,
                }
            )
    df = DataFrame(rows)
    df["rsi"] = 25.0 if oversold else 60.0
    df["macd"] = -0.5 if oversold else 0.5
    return df


def make_range_bound_df(n: int = 50) -> DataFrame:
    """
    Create a choppy, range-bound OHLCV dataframe suitable for grid trading.
    The last candle sits near the lower half of the range to represent a dip.
    """
    rows = []
    base_price = 100.0
    pattern = [0.6, -0.5, 0.4, -0.6, 0.5, -0.4, 0.3, -0.7]
    price = base_price
    for i in range(n):
        price = base_price + pattern[i % len(pattern)]
        rows.append(
            {
                "open": price * 1.001,
                "high": price * 1.008,
                "low": price * 0.992,
                "close": price,
                "volume": 150.0,
                "close_time": i * 300_000,
                "number_of_trades": 12,
            }
        )
    rows[-1]["close"] = 99.2
    rows[-1]["open"] = 99.5
    rows[-1]["high"] = 100.0
    rows[-1]["low"] = 98.9
    return DataFrame(rows)


# ---------------------------------------------------------------------------
# _compute_mfi unit tests
# ---------------------------------------------------------------------------


def test_compute_mfi_returns_float():
    df = make_ohlcv_df(n=50, oversold=False)
    result = PriceTracker._compute_mfi(df)
    assert isinstance(result, float)


def test_compute_mfi_range():
    """MFI must always be in [0, 100]."""
    df = make_ohlcv_df(n=50, oversold=False)
    result = PriceTracker._compute_mfi(df)
    assert 0 <= result <= 100


def test_compute_mfi_low_for_downtrend_low_volume():
    """Persistent downtrend with very low volume should yield low MFI."""
    df = make_ohlcv_df(n=50, oversold=True)
    result = PriceTracker._compute_mfi(df)
    assert result < 50


# ---------------------------------------------------------------------------
# price_tracker integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_tracker_skips_when_insufficient_data():
    """Algorithm must not fire when there are fewer than 30 rows."""
    df = make_ohlcv_df(n=20)
    algo = make_algo(df)
    at_mock = AsyncMock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )

    at_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_tracker_no_signal_on_uptrend():
    """No signal should be emitted when RSI is not oversold."""
    df = make_ohlcv_df(n=50, oversold=False)
    algo = make_algo(df)
    at_mock = AsyncMock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(send_signal=AsyncMock())
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_tracker_emits_signal_when_all_conditions_met(monkeypatch):
    """
    When RSI < 30, MACD < 0, and MFI < 20, the algorithm must send a signal
    and include the expected indicator details in the Telegram message.
    """
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = AsyncMock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(send_signal=tg_mock)
    )
    algo.latest_market_context = SimpleNamespace(
        market_stress_score=0.1,
        advancers_ratio=0.58,
        btc_present=True,
        fresh_count=12,
    )

    monkeypatch.setattr(
        "algorithms.coinrule.PriceTracker._compute_mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "algorithms.coinrule.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            candidate=kwargs["candidate"],
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_awaited_once()

    tg_await_args = tg_mock.await_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "&lt; 30" in telegram_msg
    assert "&lt; 0" in telegram_msg
    assert "&lt; 20" in telegram_msg


@pytest.mark.asyncio
async def test_grid_trading_emits_signal_for_range_bound_dip():
    df = make_range_bound_df(n=50)
    algo = make_grid_algo(df)
    algo.latest_market_context = SimpleNamespace(
        market_stress_score=0.1,
        advancers_ratio=0.58,
        long_tailwind=0.2,
    )
    at_mock = AsyncMock()
    tg_mock = AsyncMock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(send_signal=tg_mock)
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        rsi_value=34.0,
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_awaited_once()

    telegram_msg = tg_mock.await_args.args[0]
    assert "coinrule_grid_trading" in telegram_msg
    assert "Sell 10% every +2% move" in telegram_msg
    assert "RSI buy zone" in telegram_msg

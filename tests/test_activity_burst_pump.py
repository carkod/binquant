from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, SymbolModel

from market_regime.models import DerivativesPositioningFeatures
from strategies.activity_burst_pump import ActivityBurstPump


def make_context(df: DataFrame) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        dispatch_signal_record=AsyncMock(),
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
        df_5m=df,
        first_seen_at=0,
        interval=SimpleNamespace(get_ms=lambda: 60_000),
        latest_market_context=None,
        _breadth_cross_tolerance=0.05,
        _autotrade_stress_threshold=0.35,
    )


def make_algo(df: DataFrame) -> ActivityBurstPump:
    return ActivityBurstPump(cast(Any, make_context(df)))


def make_low_liquidity_df() -> DataFrame:
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

    return DataFrame(rows)


def test_compute_indicators_uses_median_baseline():
    df = make_low_liquidity_df()
    algo = make_algo(df)

    indicators = algo.compute_indicators(cast(Any, df))
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
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    monkeypatch.setattr(
        "strategies.activity_burst_pump.build_links_msg",
        lambda env, exchange, market_type, symbol: ("https://exchange", "https://bot"),
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=1.05, bb_mid=1.03, bb_low=1.01
    )

    send_signal_mock.assert_called_once()
    process_mock.assert_awaited_once()

    await_args = process_mock.await_args
    telegram_await_args = send_signal_mock.call_args
    assert await_args is not None
    assert telegram_await_args is not None
    value = await_args.args[0]
    telegram_msg = telegram_await_args.args[0]

    assert value.bot_params.name == "activity_burst_pump"
    assert value.bot_params.pair == "TESTUSDT"
    assert value.current_price == pytest.approx(1.04)
    assert value.bot_params.position == "long"
    assert "Score: 0.4" in telegram_msg


@pytest.mark.asyncio
async def test_signal_generator_skips_when_price_jump_is_too_small():
    df = make_low_liquidity_df()
    df.loc[df.index[-1], "close"] = 1.005
    df.loc[df.index[-1], "high"] = 1.006
    algo = make_algo(df)
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]), bb_high=1.05, bb_mid=1.03, bb_low=1.01
    )

    send_signal_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_generator_records_but_does_not_trade_cascade_risk(
    monkeypatch,
):
    df = make_low_liquidity_df()
    context = make_context(df)
    context.latest_market_context = SimpleNamespace(
        get_symbol_features=lambda symbol: SimpleNamespace(
            derivatives=DerivativesPositioningFeatures(
                timestamp=1_700_000_000_000,
                open_interest=1_000.0,
                open_interest_notional=100_000.0,
                derivatives_stress_score=0.8,
                positioning_state="CASCADE_RISK",
            )
        )
    )
    algo = ActivityBurstPump(cast(Any, context))
    monkeypatch.setattr(
        "strategies.activity_burst_pump.allows_long_autotrade",
        lambda context, symbol: True,
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=1.05,
        bb_mid=1.03,
        bb_low=1.01,
    )

    context.dispatch_signal_record.assert_called_once()
    dispatched = context.dispatch_signal_record.call_args.kwargs
    assert dispatched["value"].autotrade is False
    assert dispatched["indicators"]["activity_burst_entry_block_reason"] == (
        "derivatives_cascade_risk"
    )
    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()

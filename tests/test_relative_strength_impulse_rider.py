from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import (
    AutotradeSettingsSchema,
    ExchangeId,
    MarketType,
    SymbolModel,
)

from strategies.relative_strength_impulse_rider import RelativeStrengthImpulseRider


def make_candles() -> DataFrame:
    closes = [100.0] * 15 + [101.0, 102.0, 104.0, 109.0, 110.0]
    rows = []
    for index, close in enumerate(closes):
        open_time = 1_700_000_000_000 + index * 900_000
        rows.append(
            {
                "open_time": open_time,
                "close_time": open_time + 899_999,
                "open": close - 0.1,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": 100.0,
                "ATR": 2.0,
            }
        )
    rows[-2].update({"open": 104.0, "high": 110.0, "low": 103.0})
    rows[-1].update({"open": 109.2, "high": 110.2, "low": 109.0})
    return DataFrame(rows)


def append_retest_candle(
    frame: DataFrame,
    *,
    open_: float = 108.7,
    high: float = 110.0,
    low: float = 108.5,
    close: float = 109.2,
    completed: bool = True,
) -> None:
    open_time = int(frame["open_time"].iloc[-1]) + 900_000
    frame.loc[len(frame)] = {
        "open_time": open_time,
        "close_time": open_time + (899_999 if completed else 90_000_000_000),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": 100.0,
        "ATR": 2.0,
    }


def make_btc_candles(symbol_candles: DataFrame) -> DataFrame:
    frame = DataFrame(
        {
            "open_time": symbol_candles["open_time"],
            "close_time": symbol_candles["close_time"],
            "close": [100.0] * len(symbol_candles),
        }
    )
    frame.loc[frame.index[-2], "close"] = 100.5
    return frame


def make_context(
    *,
    df_15m: DataFrame | None = None,
    df_btc_15m: DataFrame | None = None,
) -> SimpleNamespace:
    symbol_candles = df_15m if df_15m is not None else make_candles()
    btc_candles = (
        df_btc_15m if df_btc_15m is not None else make_btc_candles(symbol_candles)
    )
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDTM",
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.FUTURES,
        current_symbol_data=SymbolModel(
            id="TESTUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=4,
        ),
        price_precision=4,
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=AsyncMock(),
        ),
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
        strategy_cooldowns={},
        strategy_states={},
        df_15m=symbol_candles,
        df_btc_15m=btc_candles,
    )


@pytest.mark.asyncio
async def test_signal_waits_for_completed_bullish_retest_reclaim(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()

    def finalize_to_eight(value: Any) -> None:
        value.bot_params.fiat_order_size = 8.0

    context.finalize_signal_bot_params.side_effect = finalize_to_eight
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()

    append_retest_candle(context.df_15m)
    context.df_btc_15m.loc[len(context.df_btc_15m)] = {
        "open_time": context.df_15m["open_time"].iloc[-1],
        "close_time": context.df_15m["close_time"].iloc[-1],
        "close": 100.5,
    }
    algo = RelativeStrengthImpulseRider(cast(Any, context))
    await algo.signal(109.2, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_called_once()
    context.telegram_consumer.dispatch_signal.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()
    signal = context.at_consumer.process_autotrade_restrictions.await_args.args[0]
    indicators = context.dispatch_signal_record.call_args.kwargs["indicators"]

    assert signal.autotrade is True
    assert signal.bot_params.name == "relative_strength_impulse_rider"
    assert signal.bot_params.position == "long"
    assert signal.bot_params.fiat_order_size == 8.0
    assert signal.bot_params.cooldown == 240
    assert signal.bot_params.stop_loss == 2.0
    assert signal.bot_params.take_profit == 12.0
    assert signal.bot_params.dynamic_trailing is False
    assert signal.bot_params.trailing is True
    assert signal.bot_params.trailing_profit == 5.0
    assert signal.bot_params.trailing_deviation == 2.0
    telegram_msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Max margin: 8.0 USDT" in telegram_msg
    assert indicators["retest_level"] == pytest.approx(108.9)
    assert indicators["retest_low"] == pytest.approx(108.5)
    assert indicators["retest_close"] == pytest.approx(109.2)
    assert indicators["retest_wait_bars"] == 1
    assert indicators["max_retest_invalidation_pct"] == 2.0
    assert indicators["max_holding_bars"] == 8


@pytest.mark.asyncio
async def test_signal_emits_confirmed_retest_only_once(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)
    append_retest_candle(context.df_15m)
    await algo.signal(110.0, 112.0, 105.0, 98.0)
    await algo.signal(110.0, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retest",
    [
        {"low": 109.0},
        {"close": 108.8},
        {"open_": 109.3, "close": 109.2},
        {"low": 105.0},
    ],
)
async def test_signal_rejects_unconfirmed_or_invalidated_retest(monkeypatch, retest):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)
    append_retest_candle(context.df_15m, **retest)
    await algo.signal(109.2, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert context.strategy_states == {}


@pytest.mark.asyncio
async def test_signal_ignores_forming_retest_until_candle_completes(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)
    append_retest_candle(context.df_15m, completed=False)
    await algo.signal(109.2, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_not_called()
    assert context.strategy_states != {}


@pytest.mark.asyncio
async def test_signal_expires_retest_if_next_completed_candle_was_missed(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)
    append_retest_candle(context.df_15m, low=109.0)
    append_retest_candle(context.df_15m)
    await algo.signal(109.2, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert context.strategy_states == {}


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda symbol, btc: symbol.at.__setitem__(
                (symbol.index[-3], "close"), 106.0
            ),
            "impulse_did_not_cross_threshold",
        ),
        (
            lambda symbol, btc: btc.at.__setitem__((btc.index[-2], "close"), 102.0),
            "btc_return_too_high",
        ),
        (
            lambda symbol, btc: symbol.loc.__setitem__(
                (symbol.index[-1], ["open", "high", "low", "close"]),
                [109.5, 110.0, 107.5, 108.0],
            ),
            "confirmation_did_not_hold_trigger_close",
        ),
        (
            lambda symbol, btc: symbol.at.__setitem__((symbol.index[-2], "ATR"), 7.0),
            "atr_pct_too_high",
        ),
    ],
)
def test_features_reject_invalid_trigger_or_confirmation(mutate, reason):
    symbol = make_candles()
    btc = make_btc_candles(symbol)
    mutate(symbol, btc)

    features, actual_reason = RelativeStrengthImpulseRider._features(symbol, btc)

    assert features is None
    assert actual_reason == reason


@pytest.mark.asyncio
async def test_signal_autotrades_in_production(monkeypatch):
    """
    Staging holds too little balance to ever open a position, so this
    strategy autotrades in production rather than shadowing there.
    """
    monkeypatch.setenv("ENV", "production")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)
    append_retest_candle(context.df_15m)
    await algo.signal(109.2, 112.0, 105.0, 98.0)

    signal = context.at_consumer.process_autotrade_restrictions.await_args.args[0]
    assert signal.autotrade is True

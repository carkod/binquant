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
        rows.append(
            {
                "open_time": 1_800_000_000_000 + index * 900_000,
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


def make_btc_candles(symbol_candles: DataFrame) -> DataFrame:
    frame = DataFrame(
        {
            "open_time": symbol_candles["open_time"],
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
        dispatch_signal_record=Mock(),
        strategy_cooldowns={},
        df_15m=symbol_candles,
        df_btc_15m=btc_candles,
    )


@pytest.mark.asyncio
async def test_signal_dispatches_frozen_one_percent_retest_contract(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_called_once()
    context.telegram_consumer.dispatch_signal.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()
    signal = context.at_consumer.process_autotrade_restrictions.await_args.args[0]
    indicators = context.dispatch_signal_record.call_args.kwargs["indicators"]

    assert signal.autotrade is True
    assert signal.bot_params.name == "relative_strength_impulse_rider"
    assert signal.bot_params.position == "long"
    assert signal.bot_params.fiat_order_size == 2.0
    assert signal.bot_params.cooldown == 240
    assert signal.bot_params.stop_loss == 2.0
    assert signal.bot_params.take_profit == 12.0
    assert signal.bot_params.dynamic_trailing is False
    assert signal.bot_params.trailing is True
    assert signal.bot_params.trailing_profit == 5.0
    assert signal.bot_params.trailing_deviation == 2.0
    assert indicators["entry_limit_price"] == pytest.approx(108.9)
    assert indicators["retest_wait_bars"] == 3
    assert indicators["max_holding_bars"] == 8


@pytest.mark.asyncio
async def test_signal_emits_confirmation_only_once(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)
    await algo.signal(110.0, 112.0, 105.0, 98.0)

    context.dispatch_signal_record.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


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
async def test_signal_records_shadow_only_outside_staging(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    context = make_context()
    algo = RelativeStrengthImpulseRider(cast(Any, context))

    await algo.signal(110.0, 112.0, 105.0, 98.0)

    signal = context.at_consumer.process_autotrade_restrictions.await_args.args[0]
    assert signal.autotrade is False

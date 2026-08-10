from datetime import datetime
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

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.top_gainer_early_momentum import TopGainerEarlyMomentum


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDTM",
        "timestamp": 1_000,
        "close": 111.0,
        "return_pct": 0.08,
        "ema20": 105.0,
        "ema50": 102.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.05,
        "relative_strength_vs_btc": 0.04,
        "atr_pct": 0.025,
        "bb_width": 0.05,
        "micro_regime": "TREND_UP",
        "micro_regime_strength": 0.78,
        "micro_regime_transition": "BREAKOUT_UP",
        "micro_regime_transition_strength": 0.5,
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
        "average_return": 0.01,
        "average_relative_strength_vs_btc": 0.01,
        "pct_above_ema20": 0.66,
        "pct_above_ema50": 0.61,
        "average_trend_score": 0.04,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.005,
        "btc_trend_score": 0.02,
        "btc_regime_score": 0.1,
        "long_tailwind": 0.34,
        "short_tailwind": 0.08,
        "market_regime": "TREND_UP",
        "previous_market_regime": None,
        "market_regime_transition": "ENTERED_TREND_UP",
        "market_regime_transition_strength": 0.45,
        "long_regime_score": 0.64,
        "short_regime_score": 0.2,
        "range_regime_score": 0.2,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDTM": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_breakout_candles() -> DataFrame:
    closes = [100.0] * 84
    closes.extend(
        [
            101.0,
            101.8,
            102.5,
            103.2,
            103.8,
            104.3,
            104.9,
            105.0,
            105.2,
            105.8,
            106.2,
            106.8,
            107.4,
            108.2,
            109.2,
            114.5,
        ]
    )
    rows: list[dict[str, float | int]] = []
    for index, close in enumerate(closes):
        is_last = index == len(closes) - 1
        open_price = 110.0 if is_last else close - 0.15
        high = 115.0 if is_last else close + 0.25
        low = 109.8 if is_last else close - 0.35
        volume = 230.0 if is_last else 100.0
        rows.append(
            {
                "open_time": 1_700_000_000_000 + index * 900_000,
                "close_time": 1_700_000_000_000 + (index + 1) * 900_000 - 1,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "quote_asset_volume": volume * close,
                "ATR": 1.0,
            }
        )
    source_index = len(rows) - 3
    rows[source_index].update(
        {
            "open": 107.0,
            "high": 112.2,
            "low": 106.8,
            "close": 112.0,
            "volume": 230.0,
            "quote_asset_volume": 230.0 * 112.0,
        }
    )
    rows[source_index + 1].update(
        {
            "open": 111.5,
            "high": 112.7,
            "low": 111.2,
            "close": 112.5,
            "quote_asset_volume": 100.0 * 112.5,
        }
    )
    rows[source_index + 2].update(
        {
            "open": 112.3,
            "high": 113.2,
            "low": 112.0,
            "close": 113.0,
            "quote_asset_volume": 100.0 * 113.0,
        }
    )
    return DataFrame(rows)


def make_short_history_breakout_candles() -> DataFrame:
    rows = make_breakout_candles().iloc[-64:].copy()
    rows["open_time"] = [
        1_700_000_000_000 + index * 900_000 for index in range(len(rows))
    ]
    return rows.reset_index(drop=True)


def make_context(
    *,
    df_15m: DataFrame,
    latest_market_context: LiveMarketContext,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDTM",
        market_type=MarketType.FUTURES,
        df_15m=df_15m,
        binbot_api=SimpleNamespace(dispatch_create_signal=Mock()),
        dispatch_signal_record=Mock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=AsyncMock(),
        ),
        latest_market_context=latest_market_context,
        current_symbol_data=SymbolModel(
            id="TESTUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=8,
        ),
        price_precision=8,
        exchange=ExchangeId.KUCOIN,
        strategy_cooldowns={},
    )


@pytest.mark.asyncio
async def test_signal_dispatches_long_with_reduced_margin(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    algo = TopGainerEarlyMomentum(
        cast(
            Any,
            make_context(df_15m=df, latest_market_context=make_market_context()),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_msg = send_signal_mock.call_args.args[0]
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]

    assert "Breakout setup: top_gainer_breakout_ignition" in telegram_msg
    assert "Entry setup: top_gainer_breakout_two_close_confirmation" in telegram_msg
    assert "Autotrade route: confirmed_top_gainer_long" in telegram_msg
    assert "Max margin: 2.0 USDT" in telegram_msg
    assert signal_value.autotrade is True
    assert signal_value.bot_params.position == "long"
    assert signal_value.bot_params.fiat_order_size == 2.0
    assert signal_value.bot_params.stop_loss > 0
    assert signal_value.bot_params.cooldown == 60
    assert signal_value.bot_params.trailing is True
    assert signal_value.bot_params.trailing_profit == 3.0
    assert signal_value.bot_params.trailing_deviation == 1.5


@pytest.mark.asyncio
async def test_signal_autotrades_outside_staging(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    df = make_breakout_candles()
    algo = TopGainerEarlyMomentum(
        cast(
            Any,
            make_context(df_15m=df, latest_market_context=make_market_context()),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_msg = send_signal_mock.call_args.args[0]
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]

    assert "Autotrade route: confirmed_top_gainer_long" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg
    assert signal_value.autotrade is True
    assert signal_value.bot_params.fiat_order_size == 2.0


@pytest.mark.asyncio
async def test_signal_labels_short_history_extension_window(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_short_history_breakout_candles()
    algo = TopGainerEarlyMomentum(
        cast(
            Any,
            make_context(df_15m=df, latest_market_context=make_market_context()),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    send_signal_mock.assert_called_once()
    telegram_msg = send_signal_mock.call_args.args[0]

    assert "extension return (61 bars, cap 31.77%)" in telegram_msg
    assert "24h return" not in telegram_msg


@pytest.mark.asyncio
async def test_signal_skips_short_history_when_scaled_extension_cap_is_exceeded(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "staging")
    df = make_short_history_breakout_candles()
    df.loc[df.index[0], "close"] = 75.0
    algo = TopGainerEarlyMomentum(
        cast(
            Any,
            make_context(df_15m=df, latest_market_context=make_market_context()),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_when_relative_strength_is_not_positive(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    context = make_market_context(
        symbol_features={
            "TESTUSDTM": make_symbol_features(relative_strength_vs_btc=0.0)
        }
    )
    algo = TopGainerEarlyMomentum(
        cast(Any, make_context(df_15m=df, latest_market_context=context))
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_persists_symbol_specific_risk_rejection_once_per_candle(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    market_context = make_market_context(
        symbol_features={
            "TESTUSDTM": make_symbol_features(relative_strength_vs_btc=0.0)
        }
    )
    context = make_context(
        df_15m=df,
        latest_market_context=market_context,
    )
    algo = TopGainerEarlyMomentum(cast(Any, context))

    for _ in range(2):
        await algo.signal(
            current_price=float(df.close.iloc[-1]),
            bb_high=115.0,
            bb_mid=106.0,
            bb_low=98.0,
        )

    context.binbot_api.dispatch_create_signal.assert_called_once()
    payload = context.binbot_api.dispatch_create_signal.call_args.kwargs
    indicators = payload["indicators"]

    assert payload["algorithm_name"] == "top_gainer_early_momentum"
    assert payload["symbol"] == "TESTUSDTM"
    assert payload["direction"] == "long"
    assert payload["autotrade"] is False
    assert payload["signal_kind"] == "risk_rejection"
    assert indicators["risk_reason"] == "relative_strength_vs_btc_not_positive"
    assert indicators["relative_strength_vs_btc"] == 0.0
    assert indicators["symbol_atr_pct"] == 0.025
    assert indicators["symbol_micro_regime_transition"] == "BREAKOUT_UP"
    context.dispatch_signal_record.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_rejects_confirmed_volatility_expansion(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    market_context = make_market_context(
        symbol_features={
            "TESTUSDTM": make_symbol_features(
                micro_regime_transition="VOLATILITY_EXPANSION"
            )
        }
    )
    context = make_context(
        df_15m=df,
        latest_market_context=market_context,
    )

    await TopGainerEarlyMomentum(cast(Any, context)).signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    context.dispatch_signal_record.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    payload = context.binbot_api.dispatch_create_signal.call_args.kwargs
    assert payload["signal_kind"] == "risk_rejection"
    assert payload["indicators"]["risk_reason"] == "symbol_transition_not_long"


@pytest.mark.asyncio
async def test_symbol_downtrend_remains_blocked(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    market_context = make_market_context(
        symbol_features={
            "TESTUSDTM": make_symbol_features(
                micro_regime="TREND_DOWN",
                trend_score=-0.05,
                micro_regime_transition="BREAKOUT_UP",
            )
        }
    )
    context = make_context(
        df_15m=df,
        latest_market_context=market_context,
    )

    await TopGainerEarlyMomentum(cast(Any, context)).signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    context.dispatch_signal_record.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    payload = context.binbot_api.dispatch_create_signal.call_args.kwargs
    assert payload["indicators"]["risk_reason"] == "symbol_trend_down"


@pytest.mark.parametrize(
    ("context", "features", "expected_reason"),
    [
        (
            make_market_context(market_stress_score=0.25),
            make_symbol_features(micro_regime_transition="VOLATILITY_EXPANSION"),
            "market_stress_too_high",
        ),
        (
            make_market_context(),
            make_symbol_features(
                atr_pct=0.061,
                micro_regime_transition="VOLATILITY_EXPANSION",
            ),
            "symbol_atr_too_high",
        ),
        (
            make_market_context(),
            make_symbol_features(micro_regime_transition="BREAKDOWN"),
            "symbol_transition_not_long",
        ),
    ],
)
def test_risk_profile_preserves_stress_atr_and_bearish_transition_guards(
    context: LiveMarketContext,
    features: SymbolMarketFeatures,
    expected_reason: str,
) -> None:
    assert TopGainerEarlyMomentum._risk_profile_allows(
        context=context,
        features=features,
    ) == (False, expected_reason)


@pytest.mark.asyncio
async def test_signal_skips_when_symbol_features_are_missing(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    context = make_market_context(symbol_features={})
    algo = TopGainerEarlyMomentum(
        cast(Any, make_context(df_15m=df, latest_market_context=context))
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_when_one_hour_move_is_too_extended(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    df.loc[df.index[-3], ["open", "high", "low", "close"]] = [
        132.0,
        140.5,
        131.8,
        139.0,
    ]
    df.loc[df.index[-3], "quote_asset_volume"] = (
        df.loc[df.index[-3], "volume"] * df.loc[df.index[-3], "close"]
    )
    algo = TopGainerEarlyMomentum(
        cast(
            Any,
            make_context(df_15m=df, latest_market_context=make_market_context()),
        )
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any,
        SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=process_mock,
        ),
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=145.0,
        bb_mid=120.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


def test_entry_requires_seven_percent_six_hour_return() -> None:
    values, reason = TopGainerEarlyMomentum._features(make_breakout_candles().iloc[:-2])
    assert reason == "features_ready"
    assert values is not None
    values["return_6h"] = 0.0699

    assert TopGainerEarlyMomentum._entry_allows(values) == (
        False,
        "six_hour_move_not_confirmed",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row_offset", "open_price", "high", "low", "close"),
    [
        (-2, 106.5, 107.0, 106.0, 106.8),
        (-1, 111.8, 112.0, 111.0, 111.9),
    ],
)
async def test_signal_requires_both_confirmation_closes(
    monkeypatch,
    row_offset: int,
    open_price: float,
    high: float,
    low: float,
    close: float,
) -> None:
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    df.loc[df.index[row_offset], ["open", "high", "low", "close"]] = [
        open_price,
        high,
        low,
        close,
    ]
    context = make_context(df_15m=df, latest_market_context=make_market_context())

    await TopGainerEarlyMomentum(cast(Any, context)).signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    context.dispatch_signal_record.assert_not_called()
    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_does_not_use_forming_candle_as_second_confirmation(
    monkeypatch,
) -> None:
    monkeypatch.setenv("ENV", "staging")
    df = make_breakout_candles()
    df.loc[df.index[-1], "close_time"] = (
        int(datetime.now().timestamp() * 1000) + 900_000
    )
    context = make_context(df_15m=df, latest_market_context=make_market_context())

    await TopGainerEarlyMomentum(cast(Any, context)).signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=115.0,
        bb_mid=106.0,
        bb_low=98.0,
    )

    context.dispatch_signal_record.assert_not_called()
    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


def test_confirmation_requires_second_close_to_retain_momentum() -> None:
    assert TopGainerEarlyMomentum._confirmation_allows(
        breakout_close=112.0,
        previous_high=111.0,
        first_confirmation_close=112.5,
        second_confirmation_open=112.4,
        second_confirmation_high=112.7,
        second_confirmation_low=111.8,
        second_confirmation_close=112.2,
    ) == (False, "second_confirmation_did_not_retain_momentum")

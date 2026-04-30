from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType
from unittest.mock import AsyncMock, MagicMock, Mock

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.coinrule.buy_the_dip import BuyTheDip


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


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 97.0,
        "return_pct": -0.03,
        "ema20": 98.0,
        "ema50": 99.0,
        "above_ema20": False,
        "above_ema50": False,
        "trend_score": -0.02,
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
        "advancers_ratio": 0.55,
        "decliners_ratio": 0.45,
        "advancers": 28,
        "decliners": 22,
        "advancers_decliners_ratio": 28 / 22,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "XBTUSDTM",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.003,
        "average_relative_strength_vs_btc": 0.001,
        "pct_above_ema20": 0.58,
        "pct_above_ema50": 0.52,
        "average_trend_score": 0.01,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.004,
        "btc_trend_score": 0.02,
        "btc_regime_score": 0.1,
        "long_tailwind": 0.25,
        "short_tailwind": 0.1,
        "market_regime": "RANGE",
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.45,
        "short_regime_score": 0.2,
        "range_regime_score": 0.7,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_algo(
    df_15m: DataFrame,
    latest_market_context: LiveMarketContext | None = None,
) -> tuple[BuyTheDip, SimpleNamespace]:
    binbot_api = MagicMock()
    binbot_api.get_bots_by_name.return_value = []
    context = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.SPOT,
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        latest_market_context=latest_market_context,
        df_15m=df_15m,
        binbot_api=binbot_api,
    )
    return BuyTheDip(cast(Any, context)), context


@pytest.mark.asyncio
async def test_buy_the_dip_enters_on_valid_six_hour_dip() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
    algo, context = make_algo(make_buy_the_dip_df(closes))

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()

    telegram_msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "coinrule_buy_the_dip" in telegram_msg
    assert "Action: LONG ENTRY" in telegram_msg
    assert "Market regime: UNAVAILABLE" in telegram_msg
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

    context.telegram_consumer.dispatch_signal.assert_not_called()
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

    context.telegram_consumer.dispatch_signal.assert_not_called()
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

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_can_emit_repeated_signals_without_local_cooldown() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
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

    assert context.telegram_consumer.dispatch_signal.call_count == 2
    assert context.at_consumer.process_autotrade_restrictions.await_count == 2


@pytest.mark.asyncio
async def test_buy_the_dip_skips_when_symbol_micro_regime_is_trend_down() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
    market_context = make_market_context()
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_skips_when_market_regime_is_trend_up() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
    market_context = make_market_context(
        market_regime="TREND_UP",
        symbol_features={
            "TESTUSDT": make_symbol_features(
                micro_regime="RANGE",
                above_ema20=False,
                ema20=96.8,
            )
        },
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_skips_when_symbol_micro_regime_is_trend_up() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
    market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="TREND_UP")}
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_requires_reclaim_above_prior_close_and_ema20() -> None:
    closes = [100.0] + [96.0] * 22 + [97.6, 97.0]
    market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="RANGE")}
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_the_dip_autotrade_requires_range_or_transitional_context() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
    market_context = make_market_context(
        market_regime="TRANSITIONAL",
        symbol_features={
            "TESTUSDT": make_symbol_features(
                micro_regime="TRANSITIONAL",
            )
        },
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    process_args = context.at_consumer.process_autotrade_restrictions.await_args
    assert process_args is not None
    value = process_args.args[0]
    assert value.autotrade is True
    assert value.bot_params.margin_short_reversal is False


@pytest.mark.asyncio
async def test_buy_the_dip_skips_when_market_regime_is_trend_down() -> None:
    closes = [100.0] + [96.0] * 23 + [97.0]
    market_context = make_market_context(
        market_regime="TREND_DOWN",
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="RANGE")},
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    context.binbot_api.get_bots_by_name.assert_not_called()
    context.binbot_api.deactivate_bot.assert_not_called()


@pytest.mark.asyncio
async def test_buy_the_dip_raises_for_invalid_close_time() -> None:
    df = make_buy_the_dip_df([100.0] + [99.8] * 23 + [97.0])
    df["close_time"] = df["close_time"].astype(object)
    df.loc[df.index[-1], "close_time"] = "invalid"
    algo, _ = make_algo(df)

    with pytest.raises(ValueError):
        await algo.signal(
            current_price=97.0,
            bb_high=102.0,
            bb_mid=100.0,
            bb_low=98.0,
        )


@pytest.mark.asyncio
async def test_buy_the_dip_does_not_deactivate_active_bot_when_market_regime_turns_trend_down() -> (
    None
):
    closes = [100.0] + [96.0] * 23 + [97.0]
    market_context = make_market_context(
        market_regime="TREND_DOWN",
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="RANGE")},
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )
    context.binbot_api.get_bots_by_name.return_value = [{"id": "bot-123"}]

    await algo.signal(
        current_price=97.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    context.binbot_api.get_bots_by_name.assert_not_called()
    context.binbot_api.deactivate_bot.assert_not_called()


@pytest.mark.asyncio
async def test_buy_the_dip_does_not_deactivate_active_bot_when_reclaim_is_lost() -> (
    None
):
    closes = [100.0] + [96.0] * 22 + [97.6, 96.0]
    market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="RANGE")}
    )
    algo, context = make_algo(
        make_buy_the_dip_df(closes),
        latest_market_context=market_context,
    )
    context.binbot_api.get_bots_by_name.return_value = [{"id": "bot-123"}]

    await algo.signal(
        current_price=96.0,
        bb_high=102.0,
        bb_mid=100.0,
        bb_low=98.0,
    )

    context.telegram_consumer.dispatch_signal.assert_not_called()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    context.binbot_api.get_bots_by_name.assert_not_called()
    context.binbot_api.deactivate_bot.assert_not_called()

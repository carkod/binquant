from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import (
    AutotradeSettingsSchema,
    ExchangeId,
    GainerLoserEntry,
    GainersLosersSnapshot,
    MarketType,
    SymbolModel,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.top_loser_early_momentum import TopLoserEarlyMomentum


def make_breakdown_candles() -> DataFrame:
    closes = [100.0] * 84 + [
        99.0,
        98.2,
        97.5,
        96.8,
        96.2,
        95.7,
        95.1,
        95.0,
        94.8,
        94.2,
        93.8,
        93.2,
        92.6,
        91.8,
        90.8,
        85.5,
    ]
    rows: list[dict[str, float | int]] = []
    for index, close in enumerate(closes):
        rows.append(
            {
                "open_time": 1_700_000_000_000 + index * 900_000,
                "close_time": 1_700_000_000_000 + (index + 1) * 900_000 - 1,
                "open": close + 0.15,
                "high": close + 0.35,
                "low": close - 0.25,
                "close": close,
                "volume": 100.0,
                "quote_asset_volume": 100.0 * close,
                "ATR": 1.0,
            }
        )
    rows[-3].update(
        open=93.0,
        high=93.2,
        low=87.8,
        close=88.0,
        volume=230.0,
        quote_asset_volume=230.0 * 88.0,
    )
    rows[-2].update(
        open=88.5,
        high=88.8,
        low=87.3,
        close=87.5,
        quote_asset_volume=100.0 * 87.5,
    )
    rows[-1].update(
        open=87.7,
        high=88.0,
        low=86.8,
        close=87.0,
        quote_asset_volume=100.0 * 87.0,
    )
    return DataFrame(rows)


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDTM",
        "timestamp": 1_000,
        "close": 87.0,
        "return_pct": -0.08,
        "ema20": 92.0,
        "ema50": 96.0,
        "above_ema20": False,
        "above_ema50": False,
        "trend_score": -0.05,
        "relative_strength_vs_btc": -0.04,
        "return_pct_horizon": -0.20,
        "relative_strength_vs_btc_horizon": -0.19,
        "atr_pct": 0.025,
        "bb_width": 0.05,
        "micro_regime": "TREND_DOWN",
        "micro_regime_strength": 0.78,
        "micro_regime_transition": "BREAKDOWN",
        "micro_regime_transition_strength": 0.5,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.38,
        "decliners_ratio": 0.62,
        "advancers": 19,
        "decliners": 31,
        "advancers_decliners_ratio": 19 / 31,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDTM",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": -0.01,
        "average_relative_strength_vs_btc": -0.01,
        "pct_above_ema20": 0.34,
        "pct_above_ema50": 0.39,
        "average_trend_score": -0.04,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": -0.005,
        "btc_trend_score": -0.02,
        "btc_regime_score": -0.1,
        "long_tailwind": 0.08,
        "short_tailwind": 0.34,
        "market_regime": "TREND_DOWN",
        "previous_market_regime": None,
        "market_regime_transition": "ENTERED_TREND_DOWN",
        "long_regime_score": 0.2,
        "short_regime_score": 0.64,
        "range_regime_score": 0.2,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDTM": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_context(
    df: DataFrame,
    snapshots: list[GainersLosersSnapshot] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        gainers_losers_series=snapshots or [],
        symbol="TESTUSDTM",
        market_type=MarketType.FUTURES,
        df_15m=df,
        binbot_api=SimpleNamespace(dispatch_create_signal=Mock()),
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=AsyncMock(),
        ),
        latest_market_context=make_market_context(),
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


def make_top_loser_snapshots(count: int) -> list[GainersLosersSnapshot]:
    return [
        GainersLosersSnapshot(
            source="kucoin_futures",
            recorded_at=f"2026-08-26T{11 - index:02d}:11:34+01:00",
            top_gainers=[],
            top_losers=[
                GainerLoserEntry(
                    symbol="TESTUSDTM",
                    price_change_percent=-35.0,
                )
            ],
        )
        for index in range(count)
    ]


def test_breakdown_features_pass_symmetric_entry_rules() -> None:
    values, reason = TopLoserEarlyMomentum._features(make_breakdown_candles().iloc[:-2])

    assert reason == "features_ready"
    assert values is not None
    assert values["close"] < values["previous_low"]
    assert TopLoserEarlyMomentum._entry_allows(values) == (
        True,
        "top_loser_breakdown_ignition",
    )


def test_confirmation_requires_two_lower_closes_near_the_low() -> None:
    assert TopLoserEarlyMomentum._confirmation_allows(
        breakdown_close=88.0,
        previous_low=90.0,
        first_confirmation_close=87.5,
        second_confirmation_open=87.7,
        second_confirmation_high=88.0,
        second_confirmation_low=86.8,
        second_confirmation_close=87.0,
    ) == (True, "top_loser_breakdown_two_close_confirmation")


def test_risk_profile_requires_negative_btc_relative_strength() -> None:
    context = make_market_context()

    assert TopLoserEarlyMomentum._risk_profile_allows(
        context=context,
        features=make_symbol_features(),
    ) == (True, "risk_profile_allows_short")
    assert TopLoserEarlyMomentum._risk_profile_allows(
        context=context,
        features=make_symbol_features(relative_strength_vs_btc_horizon=-0.029),
    ) == (False, "relative_strength_vs_btc_not_negative")


@pytest.mark.asyncio
async def test_signal_dispatches_confirmed_short(monkeypatch) -> None:
    monkeypatch.setenv("ENV", "production")
    context = make_context(make_breakdown_candles())

    await TopLoserEarlyMomentum(cast(Any, context)).signal(
        current_price=87.0,
        bb_high=102.0,
        bb_mid=95.0,
        bb_low=86.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.direction == "SHORT"
    assert value.bot_params.position == "short"
    assert value.bot_params.stop_loss == 2.0
    assert value.bot_params.trailing_profit == 6.0
    assert value.bot_params.trailing_deviation == 2.5
    assert indicators["route_reason"] == "confirmed_top_loser_short"
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_sustained_top_loser_enters_on_breakdown_candle(monkeypatch) -> None:
    monkeypatch.setenv("ENV", "production")
    df = make_breakdown_candles().iloc[:-2]
    context = make_context(
        df,
        make_top_loser_snapshots(
            TopLoserEarlyMomentum.MIN_SUSTAINED_TOP_LOSER_SNAPSHOTS
        ),
    )

    await TopLoserEarlyMomentum(cast(Any, context)).signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=102.0,
        bb_mid=95.0,
        bb_low=86.0,
    )

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["entry_reason"] == "sustained_top_loser_breakdown"
    assert indicators["first_confirmation_close"] is None
    assert indicators["route_reason"] == "sustained_top_loser_short"

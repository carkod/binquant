from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import (
    AutotradeSettingsSchema,
    ExchangeId,
    MarketBreadthSeries,
    MarketType,
    Position,
    SymbolModel,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.spike_hunter_v3_kucoin import SpikeHunterV3KuCoin


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.02,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.03,
        "relative_strength_vs_btc": 0.02,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "TREND_UP",
        "micro_regime_strength": 0.82,
        "micro_regime_transition": "ENTERED_TREND_UP",
        "micro_regime_transition_strength": 0.4,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.64,
        "decliners_ratio": 0.36,
        "advancers": 32,
        "decliners": 18,
        "advancers_decliners_ratio": 32 / 18,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.013,
        "average_relative_strength_vs_btc": 0.01,
        "pct_above_ema20": 0.68,
        "pct_above_ema50": 0.64,
        "average_trend_score": 0.05,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.01,
        "btc_trend_score": 0.03,
        "btc_regime_score": 0.16,
        "long_tailwind": 0.36,
        "short_tailwind": 0.04,
        "market_regime": "TREND_UP",
        "previous_market_regime": None,
        "market_regime_transition": "ENTERED_TREND_UP",
        "market_regime_transition_strength": 0.45,
        "long_regime_score": 0.71,
        "short_regime_score": 0.18,
        "range_regime_score": 0.24,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_market_breadth_data(
    *,
    latest: float,
    previous: float,
    key: str = "market_breadth_ma",
) -> MarketBreadthSeries:
    market_breadth = [latest, previous] if key == "market_breadth" else [0.0, 0.0]
    market_breadth_ma = (
        [latest, previous] if key == "market_breadth_ma" else [None, None]
    )
    return MarketBreadthSeries(
        timestamp=[
            "2026-07-04 00:15:00",
            "2026-07-04 00:00:00",
        ],
        advancers=[32, 30],
        decliners=[18, 20],
        market_breadth=market_breadth,
        market_breadth_ma=market_breadth_ma,
        avg_gain=[0.02, 0.01],
        avg_loss=[-0.01, -0.02],
        total_volume=[1000, 900],
        strength_index=[0.2, 0.1],
    )


def make_market_breadth_data_for_context(
    context: LiveMarketContext | None,
) -> MarketBreadthSeries | None:
    if context is None:
        return None
    if context.market_regime == "TREND_UP":
        return make_market_breadth_data(latest=0.12, previous=0.10)
    if context.market_regime == "TREND_DOWN":
        return make_market_breadth_data(latest=0.10, previous=0.12)
    return make_market_breadth_data(latest=0.10, previous=0.10)


def make_context(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None,
    market_breadth_data: MarketBreadthSeries | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        market_type=MarketType.SPOT,
        df_15m=df,
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
        market_breadth_data=(
            market_breadth_data
            if market_breadth_data is not None
            else make_market_breadth_data_for_context(latest_market_context)
        ),
        current_symbol_data=SymbolModel(
            id="TESTUSDT",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=8,
        ),
        price_precision=8,
        fiat_order_size=25,
        exchange=ExchangeId.KUCOIN,
    )


def make_algo(
    latest_market_context: LiveMarketContext | None,
    market_breadth_data: MarketBreadthSeries | None = None,
) -> tuple[SpikeHunterV3KuCoin, DataFrame]:
    df = DataFrame(
        [
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.8,
                "volume": 120.0,
                "quote_asset_volume": 12_000.0,
            }
        ]
    )
    algo = SpikeHunterV3KuCoin(
        cast(
            Any,
            make_context(
                df,
                latest_market_context,
                market_breadth_data=market_breadth_data,
            ),
        )
    )
    return algo, df


def make_last_spike(
    *,
    upward: bool = True,
    downward: bool = False,
    **overrides: Any,
) -> dict[str, Any]:
    values = {
        "timestamp": "2026-04-15 00:00:00",
        "close": 100.8,
        "close_open_ratio": 0.008,
        "label": 1,
        "label_pre": 1,
        "label_short": 0,
        "label_short_pre": 0,
        "early_proba_aug_flag": 0,
        "volume_cluster_flag": True,
        "price_break_flag": False,
        "cumulative_price_break_flag": True,
        "accel_spike_flag": False,
        "cumulative_price_break_short_flag": False,
        "accel_spike_short_flag": False,
        "signal_type": "FinalSpike",
        "volume": 120.0,
        "quote_asset_volume": 12_000.0,
        "upward": upward,
        "downward": downward,
    }
    values.update(overrides)
    return values


def test_symbol_spike_rejects_volume_cluster_without_price_impulse():
    confirmed, reason = SpikeHunterV3KuCoin.symbol_spike_confirms_direction(
        make_last_spike(
            price_break_flag=False,
            cumulative_price_break_flag=False,
            accel_spike_flag=False,
            volume_cluster_flag=True,
        ),
        Position.long,
    )

    assert confirmed is False
    assert reason == "symbol_price_impulse_missing"


def test_symbol_spike_rejects_unlabelled_or_overextended_impulse():
    unlabelled = make_last_spike(label=0, price_break_flag=True)
    extended = make_last_spike(close_open_ratio=0.061, price_break_flag=True)

    assert SpikeHunterV3KuCoin.symbol_spike_confirms_direction(
        unlabelled, Position.long
    ) == (False, "symbol_price_impulse_missing")
    assert SpikeHunterV3KuCoin.symbol_spike_confirms_direction(
        extended, Position.long
    ) == (False, "symbol_spike_too_extended")


def test_breadth_route_requires_half_point_momentum_and_long_market_edge():
    context = make_market_context()
    weak_algo, _ = make_algo(
        context,
        market_breadth_data=make_market_breadth_data(latest=0.104, previous=0.10),
    )
    opposing_algo, _ = make_algo(
        make_market_context(long_regime_score=0.2, short_regime_score=0.3),
        market_breadth_data=make_market_breadth_data(latest=0.12, previous=0.10),
    )

    assert weak_algo.breadth_momentum_direction(context) == (
        None,
        "breadth_momentum_flat",
    )
    assert opposing_algo.breadth_momentum_direction(
        opposing_algo.ti.latest_market_context
    ) == (None, "market_context_not_long")


def test_post_spike_cooldown_suppresses_labels_for_eight_bars():
    algo, _ = make_algo(make_market_context())
    algo.df_15m = cast(
        Any,
        DataFrame(
            {
                "label": [1, 0, 0, 0, 1, 0, 0, 0, 0, 1],
                "label_short": [0] * 10,
            }
        ),
    )

    algo.apply_cooldown()

    assert algo.df_15m["label"].tolist() == [1, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    assert algo.df_15m["suppressed_label"].tolist() == [0, 0, 0, 0, 1, 0, 0, 0, 0, 0]


@pytest.mark.asyncio
async def test_signal_dispatches_staging_long_with_reduced_margin_when_breadth_momentum_rises_with_upward_spike(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "staging")
    context = make_market_context(market_regime="TREND_DOWN")
    algo, df = make_algo(
        context,
        market_breadth_data=make_market_breadth_data(latest=0.12, previous=0.10),
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
    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_msg = send_signal_mock.call_args.args[0]
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]

    assert "Autotrade route: staging_long_only_reenable" in telegram_msg
    assert "Max margin: 2.0 USDT" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg
    assert signal_value.autotrade is True
    assert signal_value.bot_params.position == "long"
    assert signal_value.bot_params.fiat_order_size == 2.0
    assert signal_value.bot_params.cooldown == 120
    assert signal_value.bot_params.dynamic_trailing is False
    assert signal_value.bot_params.stop_loss == 4.0
    assert signal_value.bot_params.take_profit == 6.0
    assert signal_value.bot_params.trailing is False


@pytest.mark.asyncio
async def test_signal_dispatches_shadow_long_outside_staging(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "production")
    context = make_market_context(market_regime="TREND_DOWN")
    algo, df = make_algo(
        context,
        market_breadth_data=make_market_breadth_data(latest=0.12, previous=0.10),
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
    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_called_once()
    record_mock.assert_called_once()
    process_mock.assert_awaited_once()
    telegram_msg = send_signal_mock.call_args.args[0]
    await_args = process_mock.await_args
    assert await_args is not None
    signal_value = await_args.args[0]

    assert "Autotrade route: staging_only_long_shadow" in telegram_msg
    assert "Autotrade is disabled" in telegram_msg
    assert signal_value.autotrade is False
    assert signal_value.bot_params.position == "long"
    assert signal_value.bot_params.fiat_order_size == 2.0


@pytest.mark.asyncio
async def test_signal_accepts_market_breadth_series_model(monkeypatch):
    context = make_market_context(market_regime="TREND_DOWN")
    algo, df = make_algo(
        context,
        market_breadth_data=cast(
            Any,
            make_market_breadth_data(latest=0.12, previous=0.10),
        ),
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
    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_called_once()
    process_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_signal_skips_short_when_breadth_momentum_falls_with_downward_spike(
    monkeypatch,
):
    monkeypatch.setenv("ENV", "staging")
    context = make_market_context(
        market_regime="TREND_UP",
        market_regime_transition="ENTERED_TREND_UP",
        symbol_features={
            "TESTUSDT": make_symbol_features(
                micro_regime="TREND_UP",
                micro_regime_transition="BREAKOUT_UP",
            )
        },
    )
    algo, df = make_algo(
        context,
        market_breadth_data=make_market_breadth_data(latest=0.10, previous=0.12),
    )
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)
    monkeypatch.setattr(
        algo,
        "latest_signal",
        lambda: make_last_spike(upward=False, downward=True),
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_flat_breadth_momentum_without_alert_only_signal(
    monkeypatch,
):
    context = make_market_context(market_regime="RANGE")
    algo, df = make_algo(context)
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)
    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("context", "last_spike"),
    [
        (
            make_market_context(market_regime="TREND_UP"),
            make_last_spike(upward=False, downward=True),
        ),
        (
            make_market_context(market_regime="TREND_DOWN"),
            make_last_spike(upward=True, downward=False),
        ),
    ],
)
async def test_signal_skips_when_symbol_spike_does_not_match_market_direction(
    monkeypatch,
    context,
    last_spike,
):
    algo, df = make_algo(context)
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)
    monkeypatch.setattr(algo, "latest_signal", lambda: last_spike)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_skips_high_stress_market(monkeypatch):
    context = make_market_context(
        market_regime="TREND_DOWN",
        market_stress_score=0.35,
    )
    algo, df = make_algo(context)
    send_signal_mock = Mock()
    process_mock = AsyncMock()
    record_mock = Mock()
    algo.telegram_consumer = cast(
        Any, SimpleNamespace(dispatch_signal=send_signal_mock)
    )
    algo.at_consumer = cast(
        Any, SimpleNamespace(process_autotrade_restrictions=process_mock)
    )
    monkeypatch.setattr(algo.ti, "dispatch_signal_record", record_mock)
    monkeypatch.setattr(
        algo,
        "latest_signal",
        lambda: make_last_spike(upward=False, downward=True),
    )

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=110.0,
        bb_mid=105.0,
        bb_low=100.0,
    )

    send_signal_mock.assert_not_called()
    record_mock.assert_not_called()
    process_mock.assert_not_awaited()

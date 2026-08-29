from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame, concat
from pybinbot import (
    AutotradeSettingsSchema,
    ExchangeId,
    MarketBreadthSeries,
    MarketType,
    SymbolModel,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.failed_spike_fade import FailedSpikeFade


def make_symbol_features() -> SymbolMarketFeatures:
    return SymbolMarketFeatures(
        symbol="TESTUSDT",
        timestamp=1_000,
        close=100.0,
        return_pct=0.02,
        ema20=99.5,
        ema50=99.0,
        above_ema20=True,
        above_ema50=True,
        trend_score=0.03,
        relative_strength_vs_btc=0.02,
        atr_pct=0.02,
        bb_width=0.04,
        micro_regime="TREND_UP",
    )


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
        "long_regime_score": 0.71,
        "short_regime_score": 0.18,
        "range_regime_score": 0.24,
        "stress_regime_score": 0.1,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_market_breadth_data(*, latest: float, previous: float) -> MarketBreadthSeries:
    return MarketBreadthSeries(
        timestamp=["2026-07-04 00:15:00", "2026-07-04 00:00:00"],
        advancers=[32, 30],
        decliners=[18, 20],
        market_breadth=[0.0, 0.0],
        market_breadth_ma=[latest, previous],
        avg_gain=[0.02, 0.01],
        avg_loss=[-0.01, -0.02],
        total_volume=[1000, 900],
        strength_index=[0.2, 0.1],
    )


def source_candles() -> DataFrame:
    return DataFrame(
        [
            {
                "open_time": 1_000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.8,
                "volume": 120.0,
                "quote_asset_volume": 12_000.0,
            }
        ]
    )


def failure_candle(open_time: int = 901_000, high: float = 101.2) -> DataFrame:
    return DataFrame(
        [
            {
                "open_time": open_time,
                "open": 101.0,
                "high": high,
                "low": 100.2,
                "close": 100.5,
                "volume": 90.0,
                "quote_asset_volume": 9_000.0,
            }
        ]
    )


def make_last_spike(**overrides: Any) -> dict[str, Any]:
    values = {
        "timestamp": "2026-04-15 00:00:00",
        "close": 100.8,
        "close_open_ratio": 0.008,
        "label": 1,
        "price_break_flag": False,
        "cumulative_price_break_flag": True,
        "accel_spike_flag": False,
        "volume": 120.0,
        "quote_asset_volume": 12_000.0,
        "upward": True,
    }
    values.update(overrides)
    return values


def make_algo() -> FailedSpikeFade:
    df = source_candles()
    context = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        market_type=MarketType.FUTURES,
        df_15m=df,
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT", base_order_size=6.0
            ),
            process_autotrade_restrictions=AsyncMock(),
        ),
        latest_market_context=make_market_context(),
        market_breadth_data=make_market_breadth_data(latest=0.12, previous=0.10),
        strategy_states={},
        current_symbol_data=SymbolModel(
            id="TESTUSDT",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=8,
        ),
        price_precision=8,
        exchange=ExchangeId.KUCOIN,
    )
    return FailedSpikeFade(cast(Any, context))


def state_store(algo: FailedSpikeFade) -> dict[tuple[str, str], dict[str, float | int]]:
    assert algo.strategy_states is not None
    return algo.strategy_states


def dispatch_mock(algo: FailedSpikeFade) -> Mock:
    return cast(Mock, algo.ti.dispatch_signal_record)


def process_mock(algo: FailedSpikeFade) -> AsyncMock:
    return cast(AsyncMock, algo.at_consumer.process_autotrade_restrictions)


def test_source_spike_rejects_volume_cluster_without_price_impulse():
    assert FailedSpikeFade.source_spike_allows(
        make_last_spike(
            price_break_flag=False,
            cumulative_price_break_flag=False,
            accel_spike_flag=False,
        )
    ) == (False, "symbol_price_impulse_missing")


def test_source_and_failure_require_opposite_breadth_momentum():
    algo = make_algo()
    assert algo.source_market_allows(algo.ti.latest_market_context)[0] is True
    assert algo.failure_market_allows(algo.ti.latest_market_context) == (
        False,
        "failure_breadth_not_down",
    )

    algo.market_breadth_data = make_market_breadth_data(latest=0.10, previous=0.12)

    assert algo.failure_market_allows(algo.ti.latest_market_context)[0] is True


def test_post_spike_label_cooldown_suppresses_eight_bars():
    algo = make_algo()
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


@pytest.mark.asyncio
async def test_signal_records_source_spike_without_dispatch(monkeypatch):
    algo = make_algo()
    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())

    await algo.signal(100.8, 110.0, 100.0, 105.0)

    states = state_store(algo)
    assert states[(algo.ALGO, algo.symbol)] == {
        "open_time": 1_000,
        "high": 101.0,
        "close": 100.8,
        "volume": 120.0,
        "quote_asset_volume": 12_000.0,
    }
    dispatch_mock(algo).assert_not_called()
    process_mock(algo).assert_not_awaited()


async def record_source(algo: FailedSpikeFade, monkeypatch) -> None:
    monkeypatch.setattr(algo, "latest_signal", lambda: make_last_spike())
    await algo.signal(100.8, 110.0, 100.0, 105.0)


@pytest.mark.asyncio
async def test_signal_dispatches_staging_short_after_failed_new_high(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    algo = make_algo()

    def finalize_to_eight(value: Any) -> None:
        value.bot_params.fiat_order_size = 8.0

    cast(Mock, algo.ti.finalize_signal_bot_params).side_effect = finalize_to_eight
    await record_source(algo, monkeypatch)
    algo.ti.df_15m = cast(
        Any, concat([source_candles(), failure_candle()], ignore_index=True)
    )
    algo.market_breadth_data = make_market_breadth_data(latest=0.10, previous=0.12)

    await algo.signal(100.5, 110.0, 100.0, 105.0)

    dispatch_mock(algo).assert_called_once()
    process_mock(algo).assert_awaited_once()
    await_args = process_mock(algo).await_args
    assert await_args is not None
    signal = await_args.args[0]
    assert signal.autotrade is True
    assert signal.direction == "SHORT"
    assert signal.bot_params.name == "failed_spike_fade"
    assert signal.bot_params.position == "short"
    assert signal.bot_params.fiat_order_size == 8.0
    assert signal.bot_params.stop_loss == 4.0
    assert signal.bot_params.take_profit == 6.0
    assert signal.bot_params.cooldown == 120
    assert signal.bot_params.dynamic_trailing is False
    assert signal.bot_params.trailing is False
    telegram_msg = cast(Mock, algo.telegram_consumer.dispatch_signal).call_args.args[0]
    assert "Max margin: 8.0 USDT" in telegram_msg
    assert (algo.ALGO, algo.symbol) not in state_store(algo)


@pytest.mark.asyncio
async def test_signal_keeps_waiting_without_breadth_reversal(monkeypatch):
    algo = make_algo()
    await record_source(algo, monkeypatch)
    algo.ti.df_15m = cast(
        Any, concat([source_candles(), failure_candle()], ignore_index=True)
    )

    await algo.signal(100.5, 110.0, 100.0, 105.0)

    assert (algo.ALGO, algo.symbol) in state_store(algo)
    dispatch_mock(algo).assert_not_called()


@pytest.mark.asyncio
async def test_signal_invalidates_excessive_post_spike_extension(monkeypatch):
    algo = make_algo()
    await record_source(algo, monkeypatch)
    # Derived from the cap so the rule stays protected when the cap is retuned.
    source_spike_high = float(source_candles()["high"].iloc[0])
    excessive_high = (
        source_spike_high * (1 + FailedSpikeFade.MAX_POST_SPIKE_EXTENSION) + 1.0
    )
    algo.ti.df_15m = cast(
        Any,
        concat(
            [source_candles(), failure_candle(high=excessive_high)],
            ignore_index=True,
        ),
    )
    algo.market_breadth_data = make_market_breadth_data(latest=0.10, previous=0.12)

    await algo.signal(100.5, 110.0, 100.0, 105.0)

    assert (algo.ALGO, algo.symbol) not in state_store(algo)
    dispatch_mock(algo).assert_not_called()


@pytest.mark.asyncio
async def test_signal_dispatches_shadow_short_outside_staging(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    algo = make_algo()
    await record_source(algo, monkeypatch)
    algo.ti.df_15m = cast(
        Any, concat([source_candles(), failure_candle()], ignore_index=True)
    )
    algo.market_breadth_data = make_market_breadth_data(latest=0.10, previous=0.12)

    await algo.signal(100.5, 110.0, 100.0, 105.0)

    await_args = process_mock(algo).await_args
    assert await_args is not None
    signal = await_args.args[0]
    assert signal.autotrade is False
    assert signal.bot_params.position == "short"

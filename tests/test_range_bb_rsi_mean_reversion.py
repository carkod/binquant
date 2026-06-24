from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, SymbolModel

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.range_bb_rsi_mean_reversion import RangeBbRsiMeanReversion


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.0,
        "ema20": 100.0,
        "ema50": 100.0,
        "above_ema20": False,
        "above_ema50": False,
        "trend_score": 0.0,
        "relative_strength_vs_btc": 0.0,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "RANGE",
        "micro_regime_strength": 0.7,
        "micro_regime_transition": None,
        "micro_regime_transition_strength": 0.0,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.12,
        "advancers_ratio": 0.52,
        "decliners_ratio": 0.48,
        "advancers": 26,
        "decliners": 24,
        "advancers_decliners_ratio": 26 / 24,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.0,
        "average_relative_strength_vs_btc": 0.0,
        "pct_above_ema20": 0.50,
        "pct_above_ema50": 0.50,
        "average_trend_score": 0.0,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.0,
        "btc_trend_score": 0.0,
        "btc_regime_score": 0.0,
        "long_tailwind": 0.05,
        "short_tailwind": 0.05,
        "market_regime": "RANGE",
        "previous_market_regime": "RANGE",
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.2,
        "short_regime_score": 0.2,
        "range_regime_score": 0.7,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_range_df(last_bar: dict[str, float], *, length: int = 50) -> DataFrame:
    rows = [
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "rsi": 50.0,
            "volume": 100.0,
        }
        for _ in range(length - 1)
    ]
    rows.append(last_bar)
    return DataFrame(rows)


def make_long_df() -> DataFrame:
    return make_range_df(
        {
            "open": 95.0,
            "high": 97.0,
            "low": 94.0,
            "close": 96.0,
            "rsi": 28.0,
            "volume": 250.0,
        }
    )


def make_short_df() -> DataFrame:
    return make_range_df(
        {
            "open": 105.0,
            "high": 106.0,
            "low": 103.0,
            "close": 104.0,
            "rsi": 72.0,
            "volume": 250.0,
        }
    )


def make_evaluator(
    df: DataFrame,
    latest_market_context: LiveMarketContext | None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.FUTURES,
        binbot_api=MagicMock(),
        df_15m=df,
        dispatch_signal_record=Mock(),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        latest_market_context=latest_market_context,
        current_symbol_data=SymbolModel(
            id="TESTUSDT",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=8,
            qty_precision=8,
        ),
        price_precision=8,
    )


@pytest.mark.asyncio
async def test_emits_long_on_lower_band_rsi_zscore_rejection() -> None:
    df = make_long_df()
    evaluator = make_evaluator(df, make_market_context())
    algo = RangeBbRsiMeanReversion(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=100.0,
        bb_low=95.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()

    telegram_msg = evaluator.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "range_bb_rsi_mean_reversion" in telegram_msg
    assert "Action: LONG ENTRY" in telegram_msg
    assert "lower_band_rsi_zscore_rejection" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg

    signal_value = evaluator.dispatch_signal_record.call_args.kwargs["value"]
    assert signal_value.direction == "LONG"
    assert signal_value.autotrade is True
    assert signal_value.bot_params.position == "long"

    indicators = evaluator.dispatch_signal_record.call_args.kwargs["indicators"]
    assert indicators["range_bb_rsi_entry_reason"] == (
        "lower_band_rsi_zscore_rejection"
    )


@pytest.mark.asyncio
async def test_emits_short_on_upper_band_rsi_zscore_rejection() -> None:
    df = make_short_df()
    evaluator = make_evaluator(df, make_market_context())
    algo = RangeBbRsiMeanReversion(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=100.0,
        bb_low=95.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    signal_value = evaluator.dispatch_signal_record.call_args.kwargs["value"]
    assert signal_value.direction == "SHORT"
    assert signal_value.bot_params.position == "short"

    telegram_msg = evaluator.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Action: SHORT ENTRY" in telegram_msg
    assert "upper_band_rsi_zscore_rejection" in telegram_msg


@pytest.mark.asyncio
async def test_no_signal_when_market_regime_is_not_range() -> None:
    df = make_long_df()
    context = make_market_context(market_regime="TREND_UP")
    evaluator = make_evaluator(df, context)
    algo = RangeBbRsiMeanReversion(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=100.0,
        bb_low=95.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()
    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_symbol_is_breaking_down() -> None:
    df = make_long_df()
    symbol_features = make_symbol_features(
        micro_regime_transition="BREAKDOWN",
    )
    context = make_market_context(symbol_features={"TESTUSDT": symbol_features})
    evaluator = make_evaluator(df, context)
    algo = RangeBbRsiMeanReversion(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=100.0,
        bb_low=95.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_when_adx_is_too_high(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_long_df()
    evaluator = make_evaluator(df, make_market_context())
    algo = RangeBbRsiMeanReversion(cast(Any, evaluator))
    monkeypatch.setattr(algo, "_compute_adx", lambda df, window: 50.0)

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=100.0,
        bb_low=95.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()


@pytest.mark.asyncio
async def test_no_signal_without_rejection_candle() -> None:
    df = make_range_df(
        {
            "open": 96.0,
            "high": 97.0,
            "low": 94.0,
            "close": 95.0,
            "rsi": 28.0,
            "volume": 250.0,
        }
    )
    evaluator = make_evaluator(df, make_market_context())
    algo = RangeBbRsiMeanReversion(cast(Any, evaluator))

    await algo.signal(
        current_price=float(df.close.iloc[-1]),
        bb_high=105.0,
        bb_mid=100.0,
        bb_low=95.0,
    )

    evaluator.telegram_consumer.dispatch_signal.assert_not_called()

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketDominance, MarketType, Position

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.inverse_price_tracker import InversePriceTracker


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDT",
        "timestamp": 1_000,
        "close": 100.0,
        "return_pct": 0.01,
        "ema20": 99.5,
        "ema50": 99.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.02,
        "relative_strength_vs_btc": 0.01,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "TREND_UP",
        "micro_regime_strength": 0.7,
        "micro_regime_transition": None,
        "micro_regime_transition_strength": 0.0,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": 1_000,
        "market_stress_score": 0.1,
        "advancers_ratio": 7 / 12,
        "decliners_ratio": 5 / 12,
        "advancers": 7,
        "decliners": 5,
        "advancers_decliners_ratio": 7 / 5,
        "btc_present": True,
        "fresh_count": 12,
        "total_tracked_symbols": 12,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.01,
        "average_relative_strength_vs_btc": 0.01,
        "pct_above_ema20": 0.6,
        "pct_above_ema50": 0.58,
        "average_trend_score": 0.05,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.01,
        "btc_trend_score": 0.03,
        "btc_regime_score": 0.1,
        "long_tailwind": 0.2,
        "short_tailwind": 0.05,
        "market_regime": "TREND_UP",
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.8,
        "short_regime_score": 0.1,
        "range_regime_score": 0.2,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_ohlcv_df(n: int = 50, oversold: bool = False) -> DataFrame:
    rows = []
    price = 100.0
    for i in range(n):
        price *= 0.985 if oversold else 1.005
        rows.append(
            {
                "open": price * (1.01 if oversold else 0.99),
                "high": price * 1.015,
                "low": price * 0.985,
                "close": price,
                "volume": 0.01 if oversold else 1000.0,
                "close_time": i * 300_000,
                "number_of_trades": 10,
            }
        )
    df = DataFrame(rows)
    df["rsi"] = 25.0 if oversold else 60.0
    df["macd"] = -0.5 if oversold else 0.5
    return df


def make_context(df: DataFrame, context: LiveMarketContext | None) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        binbot_api=SimpleNamespace(),
        telegram_consumer=SimpleNamespace(send_signal=AsyncMock()),
        market_type=MarketType.SPOT,
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        current_symbol_data={"base_asset": "TEST"},
        price_precision=8,
        qty_precision=8,
        df_5m=df,
        df_15m=df,
        df_1h=df,
        df_btc_15m=df,
        market_breadth_data={"adp": [0, 1, 2]},
        bot_strategy=Position.long,
        current_market_dominance=MarketDominance.NEUTRAL,
        market_domination_reversal=False,
        latest_market_context=context,
    )


def make_algo(df: DataFrame, context: LiveMarketContext | None) -> InversePriceTracker:
    return InversePriceTracker(cast(Any, make_context(df, context)))


@pytest.mark.asyncio
async def test_inverse_price_tracker_skips_when_insufficient_rows(monkeypatch):
    algo = make_algo(make_ohlcv_df(n=20, oversold=True), make_market_context())
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    await algo.signal(close_price=100.0, bb_high=101.0, bb_low=99.0, bb_mid=100.0)
    algo.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]
    algo.at_consumer.process_autotrade_restrictions.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_skips_when_not_oversold(monkeypatch):
    algo = make_algo(make_ohlcv_df(oversold=False), make_market_context())
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 60.0),
    )
    await algo.signal(close_price=100.0, bb_high=101.0, bb_low=99.0, bb_mid=100.0)
    algo.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_emits_in_trend_up_market(monkeypatch):
    context = make_market_context(
        market_regime="TREND_UP",
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="TREND_UP")},
    )
    algo = make_algo(make_ohlcv_df(oversold=True), context)

    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.25,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )

    await algo.signal(close_price=100.0, bb_high=101.0, bb_low=99.0, bb_mid=100.0)
    algo.telegram_consumer.send_signal.assert_awaited_once()  # type: ignore[attr-defined]
    algo.at_consumer.process_autotrade_restrictions.assert_awaited_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_emits_in_bullish_transitional_market(monkeypatch):
    context = make_market_context(
        market_regime="TRANSITIONAL",
        long_tailwind=0.3,
        long_regime_score=0.9,
        short_regime_score=0.2,
        range_regime_score=0.3,
        stress_regime_score=0.1,
        symbol_features={
            "TESTUSDT": make_symbol_features(
                micro_regime="TRANSITIONAL",
                trend_score=0.1,
                above_ema20=True,
                relative_strength_vs_btc=0.05,
            )
        },
    )
    algo = make_algo(make_ohlcv_df(oversold=True), context)

    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.18,
            context_score=SimpleNamespace(
                confidence=0.65,
                followthrough_score=0.1,
                adverse_excursion_risk=0.3,
            ),
        ),
    )

    await algo.signal(close_price=100.0, bb_high=101.0, bb_low=99.0, bb_mid=100.0)
    algo.telegram_consumer.send_signal.assert_awaited_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_skips_in_range_market(monkeypatch):
    context = make_market_context(market_regime="RANGE")
    algo = make_algo(make_ohlcv_df(oversold=True), context)
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )

    await algo.signal(close_price=100.0, bb_high=101.0, bb_low=99.0, bb_mid=100.0)
    algo.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_skips_in_trend_down_or_high_stress(monkeypatch):
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )

    algo_trend_down = make_algo(
        make_ohlcv_df(oversold=True),
        make_market_context(market_regime="TREND_DOWN"),
    )
    await algo_trend_down.signal(100.0, 101.0, 99.0, 100.0)
    algo_trend_down.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]

    algo_high_stress = make_algo(
        make_ohlcv_df(oversold=True),
        make_market_context(market_stress_score=0.5),
    )
    await algo_high_stress.signal(100.0, 101.0, 99.0, 100.0)
    algo_high_stress.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_skips_when_context_or_symbol_unavailable(
    monkeypatch,
):
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )

    algo_missing_context = make_algo(make_ohlcv_df(oversold=True), None)
    await algo_missing_context.signal(100.0, 101.0, 99.0, 100.0)
    algo_missing_context.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]

    context_missing_symbol = make_market_context(symbol_features={})
    algo_missing_symbol = make_algo(
        make_ohlcv_df(oversold=True), context_missing_symbol
    )
    await algo_missing_symbol.signal(100.0, 101.0, 99.0, 100.0)
    algo_missing_symbol.telegram_consumer.send_signal.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_inverse_price_tracker_uses_context_market_type(monkeypatch):
    context = make_market_context()
    algo = make_algo(make_ohlcv_df(oversold=True), context)
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.inverse_price_tracker.SignalsConsumer",
        fake_signals_consumer,
    )

    await algo.signal(100.0, 101.0, 99.0, 100.0)
    assert captured["market_type"] == MarketType.SPOT
    assert captured["autotrade"] is False


@pytest.mark.asyncio
async def test_inverse_price_tracker_telegram_message_contains_expected_text(
    monkeypatch,
):
    context = make_market_context(market_regime_transition="ENTERED_TREND_UP")
    algo = make_algo(make_ohlcv_df(oversold=True), context)

    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            context_score=SimpleNamespace(
                confidence=0.8,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )

    await algo.signal(100.0, 101.0, 99.0, 100.0)
    msg = algo.telegram_consumer.send_signal.await_args.args[0]  # type: ignore[attr-defined]
    assert "inverse_price_tracker" in msg
    assert "Action: LONG ENTRY" in msg
    assert "favor bullish continuation rather than balanced range mean reversion" in msg
    assert "Autotrade has been disabled for testing" in msg


@pytest.mark.asyncio
async def test_inverse_price_tracker_respects_context_score_thresholds(monkeypatch):
    context = make_market_context()
    algo = make_algo(make_ohlcv_df(oversold=True), context)

    monkeypatch.setattr(
        "strategies.inverse_price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.inverse_price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=0.9,
            context_score=SimpleNamespace(
                confidence=0.49,
                followthrough_score=0.0,
                adverse_excursion_risk=0.56,
            ),
        ),
    )

    await algo.signal(100.0, 101.0, 99.0, 100.0)
    algo.at_consumer.process_autotrade_restrictions.assert_not_awaited()  # type: ignore[attr-defined]

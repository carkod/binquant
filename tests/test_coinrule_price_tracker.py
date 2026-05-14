import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, Indicators, MarketDominance, MarketType, Position

from strategies.coinrule.grid_trading import GridTrading
from strategies.coinrule.price_tracker import PriceTracker
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from market_regime.models import LiveMarketContext, SymbolMarketFeatures


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
        "regime_stable_since": -3_600_000,
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
        "market_regime": "RANGE",
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.55,
        "short_regime_score": 0.18,
        "range_regime_score": 0.72,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDT": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_context(df: DataFrame) -> SimpleNamespace:
    binbot_api = MagicMock()
    binbot_api.get_bots_by_name.return_value = []
    return SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        kucoin_symbol="TEST-USDT",
        exchange=ExchangeId.KUCOIN,
        dispatch_signal_record=Mock(),
        binbot_api=binbot_api,
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
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
        first_seen_at=0,
        interval=SimpleNamespace(get_ms=lambda: 60_000),
        latest_market_context=None,
        _breadth_cross_tolerance=0.05,
        _autotrade_stress_threshold=0.35,
    )


def make_algo(df: DataFrame) -> PriceTracker:
    return PriceTracker(cast(Any, make_context(df)))


def make_grid_algo(df: DataFrame) -> GridTrading:
    return GridTrading(cast(Any, make_context(df)))


def make_ohlcv_df(n: int = 50, oversold: bool = False) -> DataFrame:
    """
    Create a minimal OHLCV dataframe.
    When oversold=True, prices trend sharply downward to produce
    RSI < 30 and negative MACD, with low volume to produce MFI < 20.
    """
    rows = []
    if oversold:
        # Simulate a strong downtrend: each close lower than previous
        price = 100.0
        for i in range(n):
            price *= 0.985  # consistent ~1.5% drop per candle
            rows.append(
                {
                    "open": price * 1.01,
                    "high": price * 1.015,
                    "low": price * 0.985,
                    "close": price,
                    "volume": 0.01,  # very low volume → low MFI
                    "close_time": i * 300_000,
                    "number_of_trades": 10,
                }
            )
    else:
        # Neutral / uptrend – RSI will not be below 30
        price = 100.0
        for i in range(n):
            price *= 1.005
            rows.append(
                {
                    "open": price * 0.99,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "volume": 1000.0,
                    "close_time": i * 300_000,
                    "number_of_trades": 10,
                }
            )
    df = DataFrame(rows)
    df["rsi"] = 25.0 if oversold else 60.0
    df["macd"] = -0.5 if oversold else 0.5
    return df


def make_range_bound_df(n: int = 50) -> DataFrame:
    """
    Create a choppy, range-bound OHLCV dataframe suitable for grid trading.
    The last candle sits near the lower half of the range to represent a dip.
    """
    rows = []
    base_price = 100.0
    pattern = [0.6, -0.5, 0.4, -0.6, 0.5, -0.4, 0.3, -0.7]
    price = base_price
    for i in range(n):
        price = base_price + pattern[i % len(pattern)]
        rows.append(
            {
                "open": price * 1.001,
                "high": price * 1.008,
                "low": price * 0.992,
                "close": price,
                "volume": 150.0,
                "close_time": i * 300_000,
                "number_of_trades": 12,
            }
        )
    rows[-1]["close"] = 99.2
    rows[-1]["open"] = 99.5
    rows[-1]["high"] = 100.0
    rows[-1]["low"] = 98.9
    df = DataFrame(rows)
    df["rsi"] = 45.0
    df.loc[df.index[-1], "rsi"] = 34.0
    return df


# ---------------------------------------------------------------------------
# Indicators.mfi unit tests
# ---------------------------------------------------------------------------


def test_compute_mfi_returns_float():
    df = make_ohlcv_df(n=50, oversold=False)
    result = Indicators.mfi(cast(Any, df))
    assert isinstance(result, float)


def test_compute_mfi_range():
    """MFI must always be in [0, 100]."""
    df = make_ohlcv_df(n=50, oversold=False)
    result = Indicators.mfi(cast(Any, df))
    assert 0 <= result <= 100


def test_compute_mfi_low_for_downtrend_low_volume():
    """Persistent downtrend with very low volume should yield low MFI."""
    df = make_ohlcv_df(n=50, oversold=True)
    result = Indicators.mfi(cast(Any, df))
    assert result < 50


# ---------------------------------------------------------------------------
# price_tracker integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_tracker_skips_when_insufficient_data():
    """Algorithm must not fire when there are fewer than 30 rows."""
    df = make_ohlcv_df(n=20)
    algo = make_algo(df)
    at_mock = AsyncMock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )

    at_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_tracker_no_signal_on_uptrend():
    """No signal should be emitted when RSI is not oversold."""
    df = make_ohlcv_df(n=50, oversold=False)
    algo = make_algo(df)
    at_mock = AsyncMock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=Mock())
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_tracker_emits_signal_when_all_conditions_met(monkeypatch):
    """
    When RSI < 30, MACD < 0, and MFI < 20, the algorithm must send a signal
    and include the expected indicator details in the Telegram message.
    """
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    algo.latest_market_context = make_market_context()

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()

    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "&lt; 30" in telegram_msg
    assert "&lt; 0" in telegram_msg
    assert "&lt; 20" in telegram_msg
    assert "Breadth stable for mean-reversion: Yes" in telegram_msg


@pytest.mark.asyncio
async def test_price_tracker_uses_context_market_type(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    algo.market_type = MarketType.SPOT
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    algo.latest_market_context = make_market_context()

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["bot_params"].market_type == MarketType.SPOT


@pytest.mark.asyncio
async def test_price_tracker_disables_autotrade_in_transitioning_market(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    algo.latest_market_context = make_market_context(
        market_regime="TRANSITIONAL",
        market_regime_transition="LOST_REGIME_EDGE",
    )

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["autotrade"] is False


@pytest.mark.asyncio
async def test_price_tracker_disables_autotrade_during_regime_transition_even_if_regime_is_favorable(
    monkeypatch,
):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    context = make_market_context(
        market_regime="TREND_UP",
        market_regime_transition="ENTERED_TREND_UP",
        regime_is_transitioning=True,
    )
    algo.latest_market_context = context
    algo.ti.latest_market_context = context

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["autotrade"] is False
    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "Market transition: ENTERED_TREND_UP" in telegram_msg
    assert "Market regime: TREND_UP" in telegram_msg
    assert "Context timestamp: 1970-01-01 00:00:01 UTC" in telegram_msg


@pytest.mark.asyncio
async def test_price_tracker_reads_latest_context_from_evaluator(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    algo.latest_market_context = make_market_context(market_regime="RANGE")
    updated_context = make_market_context(
        market_regime="TREND_UP",
        market_regime_transition="ENTERED_TREND_UP",
        regime_is_transitioning=True,
    )
    algo.ti.latest_market_context = updated_context

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["autotrade"] is False
    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "Market regime: TREND_UP" in telegram_msg
    assert "Market transition: ENTERED_TREND_UP" in telegram_msg


@pytest.mark.asyncio
async def test_price_tracker_disables_autotrade_when_market_is_trend_up(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    algo.latest_market_context = make_market_context(
        market_regime="TREND_UP",
        market_regime_transition=None,
    )

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["autotrade"] is False
    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "Autotrade route: market_regime_trend_up" in telegram_msg


@pytest.mark.asyncio
async def test_price_tracker_disables_autotrade_for_transitional_micro_regime(
    monkeypatch,
):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    algo.latest_market_context = make_market_context(
        symbol_features={
            "TESTUSDT": make_symbol_features(
                micro_regime="TRANSITIONAL",
                micro_regime_transition=None,
            )
        }
    )

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["autotrade"] is False
    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "Autotrade route: symbol_regime_transitional" in telegram_msg


@pytest.mark.asyncio
async def test_price_tracker_disables_autotrade_when_breadth_is_unstable(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    algo = make_algo(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )
    algo.latest_market_context = make_market_context(
        advancers_ratio=0.74,
        long_tailwind=0.45,
        short_tailwind=-0.1,
    )

    captured: dict[str, Any] = {}

    def fake_signals_consumer(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.Indicators.mfi",
        staticmethod(lambda df, window=14: 15.0),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.score_signal_candidate_with_context",
        lambda **kwargs: SimpleNamespace(
            adjusted_score=1.2,
            emit=True,
            context_score=SimpleNamespace(
                confidence=0.7,
                followthrough_score=0.2,
                adverse_excursion_risk=0.2,
            ),
        ),
    )
    monkeypatch.setattr(
        "strategies.coinrule.price_tracker.SignalsConsumer", fake_signals_consumer
    )

    await algo.signal(
        close_price=float(df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert captured["autotrade"] is False
    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "Autotrade route: breadth_not_stable_for_mean_reversion" in telegram_msg


@pytest.mark.asyncio
async def test_grid_trading_emits_signal_for_range_bound_dip():
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 97.8
    df.loc[df.index[-1], "open"] = 98.8
    df.loc[df.index[-1], "high"] = 99.0
    df.loc[df.index[-1], "low"] = 98.0
    df.loc[df.index[-1], "rsi"] = 34.0
    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context()
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()

    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "coinrule_grid_trading" in telegram_msg
    assert "Action: LONG BUY ALERT" in telegram_msg
    assert "Strategy: long" in telegram_msg
    assert "Anchor window: 8 candles" in telegram_msg
    assert "Autotrade candidate: Yes" in telegram_msg
    assert "Autotrade route: market_range_stable" in telegram_msg
    assert (
        "BUY $20.00 of TESTUSDT as market order using isolated margin with 3x leverage"
        in telegram_msg
    )
    assert "Move from live anchor" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_grid_trading_emits_signal_when_market_is_not_range() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 97.8
    df.loc[df.index[-1], "open"] = 98.8
    df.loc[df.index[-1], "high"] = 99.0
    df.loc[df.index[-1], "low"] = 98.0
    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context(
        market_regime="TREND_UP",
        market_regime_transition="ENTERED_TREND_UP",
    )
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade candidate: No" in telegram_msg
    assert "Autotrade route: market_regime_trend_up" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_grid_trading_emits_candidate_for_unstable_range_market() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 97.8
    df.loc[df.index[-1], "open"] = 98.8
    df.loc[df.index[-1], "high"] = 99.0
    df.loc[df.index[-1], "low"] = 98.0
    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context(regime_stable_since=500)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade candidate: Yes" in telegram_msg
    assert "Autotrade route: market_range_unstable_allowed" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_grid_trading_emits_candidate_when_coin_regime_is_not_range() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 97.8
    df.loc[df.index[-1], "open"] = 98.8
    df.loc[df.index[-1], "high"] = 99.0
    df.loc[df.index[-1], "low"] = 98.0
    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="TREND_UP")}
    )
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade candidate: Yes" in telegram_msg
    assert "Autotrade route: market_range_stable" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_grid_trading_skips_for_range_bound_rally() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-2], "close"] = 100.1
    df.loc[df.index[-1], "close"] = 102.3
    df.loc[df.index[-1], "open"] = 101.1
    df.loc[df.index[-1], "high"] = 102.6
    df.loc[df.index[-1], "low"] = 100.9
    df.loc[df.index[-1], "rsi"] = 66.0

    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context()
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()

    tg_await_args = tg_mock.call_args
    assert tg_await_args is not None
    telegram_msg = tg_await_args.args[0]
    assert "Action: SHORT SELL ALERT" in telegram_msg
    assert "Strategy: short" in telegram_msg
    assert "SELL $20.00 of TESTUSDT as market order with 3x leverage" in telegram_msg
    assert "Autotrade is enabled" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_grid_trading_deactivates_active_bot_before_buy_entry() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 97.8
    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context()
    binbot_mock = cast(MagicMock, algo.binbot_api)
    binbot_mock.get_bots_by_name.return_value = [{"id": "bot-123"}]
    tg_mock = Mock()
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    binbot_mock.get_bots_by_name.assert_called_once_with(
        name="coinrule_grid_trading",
        symbol="TESTUSDT",
    )
    binbot_mock.deactivate_bot.assert_called_once_with(
        "bot-123",
        algorithmic_close=True,
    )
    tg_mock.assert_not_called()


@pytest.mark.asyncio
async def test_grid_trading_deactivates_active_bot_before_sell_entry() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-2], "close"] = 100.1
    df.loc[df.index[-1], "close"] = 102.3
    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context()
    binbot_mock = cast(MagicMock, algo.binbot_api)
    binbot_mock.get_bots_by_name.return_value = [{"id": "bot-123"}]
    tg_mock = Mock()
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    binbot_mock.get_bots_by_name.assert_called_once_with(
        name="coinrule_grid_trading",
        symbol="TESTUSDT",
    )
    binbot_mock.deactivate_bot.assert_called_once_with(
        "bot-123",
        algorithmic_close=True,
    )
    tg_mock.assert_not_called()


@pytest.mark.asyncio
async def test_grid_trading_skips_when_move_is_below_threshold() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 100.0
    df.loc[df.index[-1], "open"] = 100.0
    df.loc[df.index[-1], "high"] = 100.5
    df.loc[df.index[-1], "low"] = 99.5
    df.loc[df.index[-1], "rsi"] = 50.0

    algo = make_grid_algo(df)
    algo.latest_market_context = make_market_context()
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_not_called()


def test_grid_trading_buy_threshold_requires_two_percent_drop() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 97.9
    df.loc[df.index[-1], "open"] = 98.8
    df.loc[df.index[-1], "high"] = 99.0
    df.loc[df.index[-1], "low"] = 98.2

    algo = make_grid_algo(df)
    decision = algo.evaluate(
        recent_window=df.tail(algo.LOOKBACK_CANDLES),
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    assert decision.should_trigger is True
    assert decision.action == "buy"


def test_grid_trading_buy_threshold_can_be_configured_at_instantiation() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 98.9

    default_algo = make_grid_algo(df)
    configured_algo = GridTrading(
        cast(Any, make_context(df)),
        buy_trigger_pct=0.01,
    )

    default_decision = default_algo.evaluate(
        recent_window=df.tail(default_algo.LOOKBACK_CANDLES),
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )
    configured_decision = configured_algo.evaluate(
        recent_window=df.tail(configured_algo.LOOKBACK_CANDLES),
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    assert default_decision.should_trigger is False
    assert configured_decision.should_trigger is True
    assert configured_decision.action == "buy"
    assert configured_decision.trigger_move_pct == 0.01


def test_grid_trading_sell_threshold_requires_two_percent_rally() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 102.3
    df.loc[df.index[-1], "open"] = 100.0
    df.loc[df.index[-1], "high"] = 102.6
    df.loc[df.index[-1], "low"] = 99.9

    algo = make_grid_algo(df)
    decision = algo.evaluate(
        recent_window=df.tail(algo.LOOKBACK_CANDLES),
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    assert decision.should_trigger is True
    assert decision.action == "sell"


def test_grid_trading_sell_threshold_can_be_configured_at_instantiation() -> None:
    df = make_range_bound_df(n=50)
    df.loc[df.index[-1], "close"] = 101.0

    default_algo = make_grid_algo(df)
    configured_algo = GridTrading(
        cast(Any, make_context(df)),
        sell_trigger_pct=0.01,
    )

    default_decision = default_algo.evaluate(
        recent_window=df.tail(default_algo.LOOKBACK_CANDLES),
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )
    configured_decision = configured_algo.evaluate(
        recent_window=df.tail(configured_algo.LOOKBACK_CANDLES),
        current_price=float(df["close"].iloc[-1]),
        bb_high=102.0,
        bb_low=98.0,
        bb_mid=100.0,
    )

    assert default_decision.should_trigger is False
    assert configured_decision.should_trigger is True
    assert configured_decision.action == "sell"
    assert configured_decision.trigger_move_pct == 0.01


def test_grid_trading_loads_runtime_threshold_override(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    runtime_dir = tmp_path / ".runtime"
    config_dir.mkdir()
    runtime_dir.mkdir()
    default_path = config_dir / "grid_thresholds.default.json"
    override_path = runtime_dir / "grid_thresholds.json"
    default_path.write_text(
        json.dumps(
            {
                "coinrule_grid_trading": {
                    "buy_trigger_pct": 0.02,
                    "sell_trigger_pct": 0.02,
                    "anchor_window_candles": 8,
                }
            }
        )
    )
    override_path.write_text(
        json.dumps(
            {
                "coinrule_grid_trading": {
                    "buy_trigger_pct": 0.015,
                    "sell_trigger_pct": 0.025,
                    "anchor_window_candles": 12,
                }
            }
        )
    )
    monkeypatch.setattr(GridTrading, "DEFAULT_CONFIG_PATH", default_path)
    monkeypatch.setattr(GridTrading, "DEFAULT_OVERRIDE_CONFIG_PATH", override_path)
    monkeypatch.setattr(GridTrading, "_config_cache", None)
    monkeypatch.setattr(GridTrading, "_config_cache_key", None)

    algo = make_grid_algo(make_range_bound_df(n=50))

    assert algo.buy_trigger_pct == 0.015
    assert algo.sell_trigger_pct == 0.025
    assert algo.ANCHOR_WINDOW_CANDLES == 12
    assert algo.LOOKBACK_CANDLES == 13


def test_grid_trading_refresh_picks_up_rewritten_runtime_file(
    tmp_path, monkeypatch
) -> None:
    config_dir = tmp_path / "config"
    runtime_dir = tmp_path / ".runtime"
    config_dir.mkdir()
    runtime_dir.mkdir()
    default_path = config_dir / "grid_thresholds.default.json"
    override_path = runtime_dir / "grid_thresholds.json"
    default_path.write_text(
        json.dumps(
            {
                "coinrule_grid_trading": {
                    "buy_trigger_pct": 0.02,
                    "sell_trigger_pct": 0.02,
                    "anchor_window_candles": 8,
                }
            }
        )
    )
    override_path.write_text(
        json.dumps(
            {
                "coinrule_grid_trading": {
                    "buy_trigger_pct": 0.02,
                    "sell_trigger_pct": 0.02,
                    "anchor_window_candles": 8,
                }
            }
        )
    )
    monkeypatch.setattr(GridTrading, "DEFAULT_CONFIG_PATH", default_path)
    monkeypatch.setattr(GridTrading, "DEFAULT_OVERRIDE_CONFIG_PATH", override_path)
    monkeypatch.setattr(GridTrading, "_config_cache", None)
    monkeypatch.setattr(GridTrading, "_config_cache_key", None)

    algo = make_grid_algo(make_range_bound_df(n=50))
    override_path.write_text(
        json.dumps(
            {
                "coinrule_grid_trading": {
                    "buy_trigger_pct": 0.03,
                    "sell_trigger_pct": 0.03,
                    "anchor_window_candles": 10,
                }
            }
        )
    )

    algo.refresh_runtime_config()

    assert algo.buy_trigger_pct == 0.03
    assert algo.sell_trigger_pct == 0.03
    assert algo.ANCHOR_WINDOW_CANDLES == 10
    assert algo.LOOKBACK_CANDLES == 11

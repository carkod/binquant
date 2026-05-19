from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, Indicators, MarketDominance, MarketType, Position

from strategies.coinrule.bb_extreme_reversion import BBExtremeReversion
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
        "relative_strength_vs_btc": 0.06,
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
        strategy_cooldowns={},
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


def make_bbex_algo(df: DataFrame) -> BBExtremeReversion:
    return BBExtremeReversion(cast(Any, make_context(df)))


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
async def test_price_tracker_disables_autotrade_without_relative_strength(
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
            "TESTUSDT": make_symbol_features(relative_strength_vs_btc=0.03)
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
    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert (
        "Autotrade route: symbol_relative_strength_vs_btc_insufficient" in telegram_msg
    )


@pytest.mark.asyncio
async def test_price_tracker_cools_down_repeated_symbol_entries(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    context = make_context(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    context.at_consumer = SimpleNamespace(process_autotrade_restrictions=at_mock)
    context.telegram_consumer = SimpleNamespace(dispatch_signal=tg_mock)
    context.latest_market_context = make_market_context()
    algo = PriceTracker(cast(Any, context))

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

    next_df = df.copy()
    next_df.loc[next_df.index[-1], "close_time"] = int(df["close_time"].iloc[-1]) + (
        5 * 60 * 1000
    )
    context.df_5m = next_df
    repeated_algo = PriceTracker(cast(Any, context))

    await repeated_algo.signal(
        close_price=float(next_df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()


@pytest.mark.asyncio
async def test_price_tracker_allows_entry_after_cooldown_window(monkeypatch):
    df = make_ohlcv_df(n=50, oversold=True)
    context = make_context(df)
    at_mock = AsyncMock()
    tg_mock = Mock()
    context.at_consumer = SimpleNamespace(process_autotrade_restrictions=at_mock)
    context.telegram_consumer = SimpleNamespace(dispatch_signal=tg_mock)
    context.latest_market_context = make_market_context()
    algo = PriceTracker(cast(Any, context))

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

    next_df = df.copy()
    next_df.loc[next_df.index[-1], "close_time"] = int(df["close_time"].iloc[-1]) + (
        PriceTracker.ENTRY_COOLDOWN_BARS * 60 * 1000
    )
    context.df_5m = next_df
    allowed_algo = PriceTracker(cast(Any, context))

    await allowed_algo.signal(
        close_price=float(next_df["close"].iloc[-1]),
        bb_high=115.0,
        bb_low=85.0,
        bb_mid=100.0,
    )

    assert at_mock.await_count == 2
    assert tg_mock.call_count == 2


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


# ---------------------------------------------------------------------------
# BBExtremeReversion (replaces the old coinrule_grid_trading strategy)
# ---------------------------------------------------------------------------


def make_bbex_df(
    n: int = 35,
    last_closes: list[float] | None = None,
) -> DataFrame:
    """Stable-choppy 15m df; the last few closes can be overridden so RSI(2)
    can be driven to a known extreme."""
    rows = []
    for i in range(n):
        close = 100.0 + (0.2 if i % 2 == 0 else -0.2)
        rows.append(
            {
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 100.0,
                "close_time": i * 900_000,
                "number_of_trades": 10,
            }
        )
    if last_closes:
        for offset, close in enumerate(reversed(last_closes)):
            row = rows[-1 - offset]
            row["close"] = close
            row["open"] = close
            row["high"] = close + 0.2
            row["low"] = close - 0.2
    return DataFrame(rows)


@pytest.mark.asyncio
async def test_bb_extreme_emits_buy_signal_at_oversold_and_below_band():
    # Two consecutive sharp drops → RSI(2) = 0. Current price below bb_low.
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
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
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "bb_extreme_reversion" in telegram_msg
    assert "Action: LONG ENTRY" in telegram_msg
    assert "Strategy: long" in telegram_msg
    assert "Autotrade candidate: Yes" in telegram_msg
    assert "Autotrade route: market_range_stable" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_bb_extreme_emits_sell_signal_at_overbought_and_above_band():
    # Two consecutive sharp rallies → RSI(2) = 100. Current price above bb_high.
    df = make_bbex_df(n=35, last_closes=[100.0, 105.0, 110.0])
    algo = make_bbex_algo(df)
    algo.latest_market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="TREND_DOWN")}
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
        current_price=110.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_awaited_once()
    at_await_args = at_mock.await_args
    assert at_await_args is not None
    signal_value = at_await_args.args[0]
    assert signal_value.bot_params.position == Position.short
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Action: SHORT ENTRY" in telegram_msg
    assert "Autotrade route: market_range_stable" in telegram_msg
    cast(Mock, algo.ti.dispatch_signal_record).assert_called_once()


@pytest.mark.asyncio
async def test_bb_extreme_skips_when_rsi_not_oversold():
    # Flat closes → RSI(2) ≈ 50. Price below bb_low but RSI doesn't confirm.
    df = make_bbex_df(n=35)
    algo = make_bbex_algo(df)
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
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_not_called()


@pytest.mark.asyncio
async def test_bb_extreme_skips_when_price_inside_band():
    # RSI(2) = 0 (sharp drops) but current_price is at bb_mid, not below bb_low.
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
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
        current_price=100.0,  # at bb_mid, well above bb_low=95
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_not_called()


@pytest.mark.asyncio
async def test_bb_extreme_blocked_in_non_range_market_regime() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
    algo.latest_market_context = make_market_context(market_regime="TREND_UP")
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade candidate: No" in telegram_msg
    assert "Autotrade route: market_regime_trend_up" in telegram_msg


@pytest.mark.asyncio
async def test_bb_extreme_blocked_when_market_stress_too_high() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
    algo.latest_market_context = make_market_context(market_stress_score=0.5)
    at_mock = AsyncMock()
    tg_mock = Mock()
    algo.at_consumer = cast(
        AutotradeConsumer, SimpleNamespace(process_autotrade_restrictions=at_mock)
    )
    algo.telegram_consumer = cast(
        TelegramConsumer, SimpleNamespace(dispatch_signal=tg_mock)
    )

    await algo.signal(
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade route: market_stress_too_high" in telegram_msg


@pytest.mark.asyncio
async def test_bb_extreme_blocks_buy_when_micro_regime_trend_down() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
    algo.latest_market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime="TREND_DOWN")}
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
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade candidate: No" in telegram_msg
    assert "Autotrade route: symbol_regime_trend_down_for_long" in telegram_msg


@pytest.mark.asyncio
async def test_bb_extreme_blocks_short_when_micro_regime_not_shortable() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 105.0, 110.0])
    algo = make_bbex_algo(df)
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
        current_price=110.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade route: symbol_regime_not_shortable" in telegram_msg


@pytest.mark.asyncio
async def test_bb_extreme_blocks_when_micro_regime_strength_too_low() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
    algo.latest_market_context = make_market_context(
        symbol_features={"TESTUSDT": make_symbol_features(micro_regime_strength=0.2)}
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
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade route: symbol_micro_regime_unstable" in telegram_msg


@pytest.mark.asyncio
async def test_bb_extreme_blocks_during_breakdown_transition() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
    algo.latest_market_context = make_market_context(
        symbol_features={
            "TESTUSDT": make_symbol_features(
                micro_regime_transition="BREAKDOWN",
                micro_regime_transition_strength=0.6,
            )
        }
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
        current_price=90.0,
        bb_high=105.0,
        bb_low=95.0,
        bb_mid=100.0,
    )

    at_mock.assert_not_awaited()
    tg_mock.assert_called_once()
    telegram_msg = tg_mock.call_args.args[0]
    assert "Autotrade route: symbol_transition_breakdown" in telegram_msg


def test_bb_extreme_evaluate_returns_no_trigger_when_band_span_invalid() -> None:
    df = make_bbex_df(n=35, last_closes=[100.0, 95.0, 90.0])
    algo = make_bbex_algo(df)
    decision = algo.evaluate(
        recent_window=df.tail(algo.LOOKBACK_CANDLES),
        current_price=90.0,
        bb_high=100.0,
        bb_mid=100.0,
        bb_low=100.0,
    )
    assert decision.should_trigger is False
    assert "Invalid Bollinger band spread" in decision.reason

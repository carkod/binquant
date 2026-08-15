from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame, Series
from pybinbot import ExchangeId, MarketType, Position, SymbolModel

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.mean_reversion_fade import MeanReversionFade

NOW_MS = 1_700_000_000_000
CANDLE_MS = 15 * 60 * 1000
BASELINE_CLOSE = 100.0
BASELINE_VOLUME = 100.0
BASELINE_ATR = 1.0


def make_symbol_features(**overrides: Any) -> SymbolMarketFeatures:
    values = {
        "symbol": "TESTUSDTM",
        "timestamp": NOW_MS,
        "close": 100.0,
        "return_pct": 0.0,
        "ema20": 100.0,
        "ema50": 100.0,
        "above_ema20": True,
        "above_ema50": True,
        "trend_score": 0.0,
        "relative_strength_vs_btc": 0.0,
        "atr_pct": 0.02,
        "bb_width": 0.04,
        "micro_regime": "RANGE",
        "micro_regime_strength": 0.6,
        "micro_regime_transition": "MEAN_REVERSION",
        "micro_regime_transition_strength": 0.4,
    }
    values.update(overrides)
    return SymbolMarketFeatures(**values)


def make_market_context(**overrides: Any) -> LiveMarketContext:
    values = {
        "timestamp": NOW_MS,
        "market_stress_score": 0.1,
        "advancers_ratio": 0.5,
        "decliners_ratio": 0.5,
        "advancers": 25,
        "decliners": 25,
        "advancers_decliners_ratio": 1.0,
        "btc_present": True,
        "fresh_count": 50,
        "total_tracked_symbols": 50,
        "coverage_ratio": 1.0,
        "btc_symbol": "XBTUSDTM",
        "confidence": 1.0,
        "is_provisional": False,
        "average_return": 0.0,
        "average_relative_strength_vs_btc": 0.0,
        "pct_above_ema20": 0.5,
        "pct_above_ema50": 0.5,
        "average_trend_score": 0.0,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.0,
        "btc_trend_score": 0.0,
        "btc_regime_score": 0.0,
        "long_tailwind": 0.0,
        "short_tailwind": 0.0,
        "market_regime": "RANGE",
        "previous_market_regime": None,
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.4,
        "short_regime_score": 0.4,
        "range_regime_score": 0.5,
        "stress_regime_score": 0.1,
        "regime_is_transitioning": False,
        "symbol_features": {"TESTUSDTM": make_symbol_features()},
        "metadata": {},
    }
    values.update(overrides)
    return LiveMarketContext(**values)


def make_df(
    *,
    count: int = 45,
    last_open: float = 99.0,
    last_close: float = 100.0,
    last_volume: float = 300.0,
    last_atr: float = 1.0,
    baseline_atr: float = BASELINE_ATR,
) -> DataFrame:
    rows: list[dict[str, float]] = []
    for index in range(count):
        is_last = index == count - 1
        open_time = NOW_MS - (count - index) * CANDLE_MS
        open_price = last_open if is_last else BASELINE_CLOSE
        close_price = last_close if is_last else BASELINE_CLOSE
        rows.append(
            {
                "open_time": open_time,
                "close_time": open_time,
                "open": open_price,
                "high": max(open_price, close_price) + 0.5,
                "low": min(open_price, close_price) - 0.5,
                "close": close_price,
                "volume": last_volume if is_last else BASELINE_VOLUME,
                "ATR": last_atr if is_last else baseline_atr,
            }
        )
    return DataFrame(rows)


def make_evaluator(
    *,
    df: DataFrame | None = None,
    latest_market_context: LiveMarketContext | None = None,
) -> SimpleNamespace:
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
            price_precision=6,
        ),
        price_precision=6,
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        strategy_cooldowns={},
        df_15m=df if df is not None else make_df(),
        latest_market_context=(
            latest_market_context
            if latest_market_context is not None
            else make_market_context()
        ),
        dispatch_signal_record=AsyncMock(),
    )


def test_rsi_is_100_not_nan_when_window_has_no_losses() -> None:
    """Regression: a window with zero losses (a monotonic rally, exactly the
    short-entry condition this strategy watches for) must resolve RSI to
    100, not NaN. Dividing by avg_loss directly (replacing 0 with NaN)
    previously poisoned the whole series in that scenario."""
    closes = Series([100.0 + i for i in range(20)])

    rsi = MeanReversionFade._rsi(closes)

    tail = rsi.iloc[14:]
    assert tail.notna().all()
    assert (tail == 100.0).all()


def patch_rsi(
    monkeypatch: pytest.MonkeyPatch, value: float, previous_value: float | None = None
) -> None:
    def rsi_series(cls, closes):  # noqa: ARG001
        series = Series([value] * len(closes), index=closes.index)
        if previous_value is not None and len(series) >= 2:
            series.iloc[-2] = previous_value
        return series

    monkeypatch.setattr(
        MeanReversionFade,
        "_rsi",
        classmethod(rsi_series),
    )


def short_setup_df(
    *,
    last_volume: float = 300.0,
    last_atr: float = 0.7,
    baseline_atr: float = 0.7,
) -> DataFrame:
    return make_df(
        last_open=101.0,
        last_close=100.0,
        last_volume=last_volume,
        last_atr=last_atr,
        baseline_atr=baseline_atr,
    )


async def emit_short(strategy: MeanReversionFade) -> None:
    await strategy.signal(
        current_price=100.0,
        bb_high=99.5,
        bb_mid=98.0,
        bb_low=95.0,
    )


@pytest.mark.asyncio
async def test_strategy_emits_fixed_target_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df())
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    call = evaluator.dispatch_signal_record.call_args
    value = call.kwargs["value"]
    indicators = call.kwargs["indicators"]
    assert value.direction == "SHORT"
    assert value.autotrade is True
    assert value.bot_params.position == Position.short
    assert value.bot_params.dynamic_trailing is False
    assert value.bot_params.trailing is False
    assert value.bot_params.take_profit == MeanReversionFade.TAKE_PROFIT_PCT
    assert value.bot_params.cooldown == MeanReversionFade.ENTRY_COOLDOWN_MINUTES
    assert value.bot_params.stop_loss == pytest.approx(1.4)
    assert value.bot_params.margin_short_reversal is False
    assert indicators["entry_reason"] == "upper_band_outside_rsi_hook_red"
    assert indicators["max_holding_bars"] == MeanReversionFade.MAX_HOLDING_BARS
    assert indicators["trend_score"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_long_setup_never_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0, previous_value=19.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=99.5)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_short_requires_close_outside_upper_band(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df())
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(current_price=100.0, bb_high=100.5, bb_mid=98.0, bb_low=95.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_short_requires_rsi_hook_down(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=79.0)
    evaluator = make_evaluator(df=short_setup_df())
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_short_rejected_above_trend_score_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    monkeypatch.setattr(
        MeanReversionFade,
        "_trend_score",
        classmethod(lambda cls, closes: cls.MAX_TREND_SCORE + 0.0001),
    )
    evaluator = make_evaluator(df=short_setup_df())
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


def test_trend_score_is_zero_for_flat_market() -> None:
    assert MeanReversionFade._trend_score(Series([100.0] * 50)) == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_entry_rejected_when_atr_stop_exceeds_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df(last_atr=0.8, baseline_atr=0.8))
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()
    assert evaluator.strategy_cooldowns == {}


@pytest.mark.asyncio
async def test_low_volume_rejects_short(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df(last_volume=50.0))
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_atr_spike_rejects_short(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df(last_atr=5.0, baseline_atr=0.7))
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_bullish_market_context_rejects_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(
        df=short_setup_df(),
        latest_market_context=make_market_context(
            long_regime_score=0.5,
            short_regime_score=0.4,
        ),
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_spot_market_never_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df())
    evaluator.market_type = MarketType.SPOT
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_missing_atr_column_never_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df().drop(columns=["ATR"]))
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_emits_once_per_candle(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0, previous_value=81.0)
    evaluator = make_evaluator(df=short_setup_df())
    strategy = MeanReversionFade(cast(Any, evaluator))

    await emit_short(strategy)
    await emit_short(strategy)

    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()

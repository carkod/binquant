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
        dispatch_signal_record=Mock(),
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


def patch_rsi(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    monkeypatch.setattr(
        MeanReversionFade,
        "_rsi",
        classmethod(
            lambda cls, closes: Series([value] * len(closes), index=closes.index)
        ),
    )


@pytest.mark.asyncio
async def test_long_entry_fires(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.telegram_consumer.dispatch_signal.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()
    call = evaluator.dispatch_signal_record.call_args
    value = call.kwargs["value"]
    indicators = call.kwargs["indicators"]
    assert value.direction == "LONG"
    assert value.autotrade is True
    assert value.bot_params.name == "mean_reversion_fade"
    assert value.bot_params.market_type == MarketType.FUTURES
    assert value.bot_params.position == Position.long
    assert value.bot_params.dynamic_trailing is True
    assert value.bot_params.margin_short_reversal is False
    assert value.bot_params.fiat_order_size == MeanReversionFade.MAX_FIAT_ORDER_SIZE
    assert value.bot_params.stop_loss == pytest.approx(
        (2.0 * 1.0 / 100.0) * 100.0, rel=1e-3
    )
    assert indicators["entry_reason"] == "lower_band_rsi_oversold_green"
    assert indicators["risk_reason"] == "risk_profile_allows_long"


@pytest.mark.asyncio
async def test_short_entry_fires(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 80.0)
    evaluator = make_evaluator(
        df=make_df(last_open=101.0, last_close=100.0, last_volume=300.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(current_price=100.0, bb_high=99.5, bb_mid=99.0, bb_low=90.0)

    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()
    call = evaluator.dispatch_signal_record.call_args
    value = call.kwargs["value"]
    indicators = call.kwargs["indicators"]
    assert value.direction == "SHORT"
    assert value.bot_params.position == Position.short
    assert indicators["entry_reason"] == "upper_band_rsi_overbought_red"


@pytest.mark.asyncio
async def test_stop_loss_percent_derived_from_atr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(
            last_open=99.0,
            last_close=100.0,
            last_volume=300.0,
            last_atr=4.0,
            baseline_atr=4.0,
        )
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    value = evaluator.dispatch_signal_record.call_args.kwargs["value"]
    # ATR at the candidate bar is 4.0, entry price 100.0 -> 2.0x ATR / price * 100
    assert value.bot_params.stop_loss == pytest.approx(8.0, rel=1e-2)


@pytest.mark.asyncio
async def test_stop_loss_percent_clamped_to_101(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(
            last_open=0.5,
            last_close=1.0,
            last_volume=300.0,
            last_atr=100.0,
            baseline_atr=100.0,
        )
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(current_price=1.0, bb_high=110.0, bb_mid=101.0, bb_low=1.5)

    value = evaluator.dispatch_signal_record.call_args.kwargs["value"]
    assert value.bot_params.stop_loss <= 101.0


@pytest.mark.asyncio
async def test_spot_market_never_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator()
    evaluator.market_type = MarketType.SPOT
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()
    evaluator.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_atr_column_never_emits(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0)
    df = make_df().drop(columns=["ATR"])
    evaluator = make_evaluator(df=df)
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_rsi_not_oversold_rejects_long(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 40.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_long_entry_rejected_when_market_context_has_short_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=1.0),
        latest_market_context=make_market_context(
            long_regime_score=0.32,
            short_regime_score=0.45,
        ),
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()
    evaluator.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_long_entry_rejected_when_symbol_is_breaking_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=1.0),
        latest_market_context=make_market_context(
            symbol_features={
                "TESTUSDTM": make_symbol_features(
                    trend_score=-0.03,
                    micro_regime="TREND_DOWN",
                    micro_regime_transition="BREAKDOWN",
                )
            }
        ),
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()
    evaluator.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_above_lower_band_rejects_long(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    # bb_low far below current close -> close > bb_low, gate fails
    await strategy.signal(current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=50.0)

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_red_candle_rejects_long(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        # close < open -> red candle
        df=make_df(last_open=101.0, last_close=100.0, last_volume=300.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_low_volume_rejects_long(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=50.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_atr_volatility_spike_rejects_long(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=5.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()


@pytest.mark.asyncio
async def test_emits_once_per_candle(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(
        df=make_df(last_open=99.0, last_close=100.0, last_volume=300.0, last_atr=1.0)
    )
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )
    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_called_once()
    evaluator.at_consumer.process_autotrade_restrictions.assert_awaited_once()

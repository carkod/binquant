from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame, Series
from pybinbot import ExchangeId, MarketType, Position, SymbolModel

from strategies.mean_reversion_fade import MeanReversionFade


NOW_MS = 1_700_000_000_000
CANDLE_MS = 15 * 60 * 1000
BASELINE_CLOSE = 100.0
BASELINE_VOLUME = 100.0
BASELINE_ATR = 1.0


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


def make_evaluator(*, df: DataFrame | None = None) -> SimpleNamespace:
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
        dispatch_signal_record=Mock(),
    )


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
    assert value.bot_params.stop_loss == pytest.approx(
        (2.0 * 1.0 / 100.0) * 100.0, rel=1e-3
    )
    assert indicators["entry_reason"] == "lower_band_rsi_oversold_green"


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
async def test_insufficient_candle_history_never_emits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_rsi(monkeypatch, 20.0)
    evaluator = make_evaluator(df=make_df(count=39))
    strategy = MeanReversionFade(cast(Any, evaluator))

    await strategy.signal(
        current_price=100.0, bb_high=110.0, bb_mid=101.0, bb_low=100.5
    )

    evaluator.dispatch_signal_record.assert_not_called()


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

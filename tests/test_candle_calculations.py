import asyncio
from threading import Event, get_ident
from typing import Any
from unittest.mock import Mock

import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pybinbot import Candles, ExchangeId

from producers.context_evaluator import ContextEvaluator

from shared.candle_calculations import CandleCalculationPool, calculate_candles


def linear_candles(count: int, interval: int = 300_000) -> list[list[Any]]:
    return [
        [
            1_735_689_600_000 + index * interval,
            99.0 + index,
            102.0 + index,
            98.0 + index,
            100.0 + index,
            1_000.0,
            1_735_689_600_000 + (index + 1) * interval - 1,
            100_000.0,
            15,
            500.0,
            50_000.0,
        ]
        for index in range(count)
    ]


def test_indicator_values_and_original_candles_are_preserved() -> None:
    candles = linear_candles(400)
    original = [row.copy() for row in candles]

    frame = calculate_candles(ExchangeId.BINANCE, candles)

    assert candles == original
    assert len(frame) == 400  # Indicator warmup rows remain until regime assessment.
    assert frame.iloc[-1]["close"] == 499.0
    assert frame.iloc[-1]["ma_7"] == 496.0
    assert frame.iloc[-1]["ma_25"] == 487.0
    assert frame.iloc[-1]["ma_100"] == 449.5
    assert frame.iloc[-1]["rsi"] == 100.0
    assert frame.iloc[-1]["ATR"] == 4.0
    assert frame["ma_100"].iloc[:99].isna().all()


@pytest.mark.asyncio
async def test_regime_assessment_keeps_warmup_rows_and_hourly_resampling() -> None:
    candles = linear_candles(150, interval=900_000)
    raw = Candles(exchange=ExchangeId.BINANCE, candles=candles)
    expected_hourly = (
        raw.resample(raw.pre_process(), "1h").dropna().reset_index(drop=True)
    )
    pool = CandleCalculationPool()
    evaluator: Any = object.__new__(ContextEvaluator)
    evaluator.calculation_pool = pool
    evaluator.exchange = ExchangeId.BINANCE
    evaluator.symbol_dependent_data = Mock()
    evaluator.refresh_grid_only_policy = Mock()
    evaluator.load_15m_algorithms = Mock()
    try:
        await evaluator.process_data(
            candles=candles, candles_15m=candles, btc_candles_15m=candles
        )
    finally:
        pool.close()

    # Only 51 rows survive MA-100 warmup, but the regime needs 96 closed rows.
    # Its calculation must still see the complete frame before post-processing.
    assert len(evaluator.df_15m) == 51
    assert evaluator.microregime_directional == "UP"
    assert evaluator.microregime_oscillation_intensity == 0.0
    assert evaluator.macroregime_directional == "UP"
    assert_frame_equal(evaluator.df_1h, expected_hourly, check_exact=True)


@pytest.mark.asyncio
async def test_capacity_is_retained_when_a_running_calculation_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    started: asyncio.Queue[int] = asyncio.Queue()
    release = Event()
    snapshots: list[int] = []

    class CandleRow(list[Any]):
        def copy(self) -> list[Any]:
            snapshots.append(self[0])
            return super().copy()

    def blocking_calculation(exchange, candles):  # noqa: ARG001
        loop.call_soon_threadsafe(started.put_nowait, get_ident())
        if not release.wait(timeout=5):
            raise TimeoutError("Test did not release calculation")
        return DataFrame({"close": [candles[0][0]]})

    monkeypatch.setattr(
        "shared.candle_calculations.calculate_candles", blocking_calculation
    )
    pool = CandleCalculationPool(workers=2)
    first = asyncio.create_task(pool.calculate(ExchangeId.BINANCE, [CandleRow([1])]))
    second_input: list[list[Any]] = [CandleRow([2])]
    second = asyncio.create_task(pool.calculate(ExchangeId.BINANCE, second_input))
    pending = asyncio.create_task(pool.calculate(ExchangeId.BINANCE, [CandleRow([3])]))
    try:
        first_worker = await asyncio.wait_for(started.get(), timeout=5)
        second_worker = await asyncio.wait_for(started.get(), timeout=5)
        assert first_worker != second_worker
        assert get_ident() not in {first_worker, second_worker}
        assert snapshots == [1, 2]
        second_input[0][0] = 999

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.sleep(0)

        # Cancelling the asyncio waiter cannot stop its worker thread. No third
        # snapshot may be copied or submitted until an actual worker finishes.
        assert snapshots == [1, 2]
        assert started.empty()
        release.set()
        second_result, pending_result = await asyncio.wait_for(
            asyncio.gather(second, pending), timeout=5
        )
        assert_frame_equal(second_result, DataFrame({"close": [2]}))
        assert_frame_equal(pending_result, DataFrame({"close": [3]}))
        assert snapshots == [1, 2, 3]
    finally:
        release.set()
        for task in (first, second, pending):
            task.cancel()
        await asyncio.gather(first, second, pending, return_exceptions=True)
        pool.close()


@pytest.mark.asyncio
async def test_failed_calculation_releases_capacity_and_closed_pool_rejects_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def calculation(exchange, candles):  # noqa: ARG001
        if candles[0][0] < 0:
            raise ValueError("Invalid candle")
        return DataFrame({"close": [candles[0][0]]})

    monkeypatch.setattr("shared.candle_calculations.calculate_candles", calculation)
    pool = CandleCalculationPool(workers=1)
    try:
        with pytest.raises(ValueError, match="Invalid candle"):
            await pool.calculate(ExchangeId.BINANCE, [[-1]])
        result = await asyncio.wait_for(
            pool.calculate(ExchangeId.BINANCE, [[42]]), timeout=5
        )
        assert_frame_equal(result, DataFrame({"close": [42]}))
    finally:
        pool.close()

    with pytest.raises(RuntimeError, match="pool is closed"):
        await pool.calculate(ExchangeId.BINANCE, [[42]])

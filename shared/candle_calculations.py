"""Independent candle calculations, with bounded submission to worker threads."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import Candles, ExchangeId, Indicators, KlineSchema


def calculate_candles(
    exchange: ExchangeId, candles: list[list[Any]]
) -> TypedDataFrame[KlineSchema]:
    """Build and enrich a frame owned exclusively by this calculation."""
    df = Candles(exchange=exchange, candles=candles).pre_process()
    if df.empty or df.close.size == 0:
        return df

    for period in (7, 25, 100):
        df = Indicators.moving_averages(df, period)
    df = Indicators.macd(df=df)
    df = Indicators.rsi(df=df)
    df = Indicators.ma_spreads(df)
    df = Indicators.bollinguer_spreads(df)
    df = Indicators.set_twap(df)
    return Indicators.atr(df=df, window=14)


class CandleCalculationPool:
    """Allow at most one submitted snapshot per worker, including cancellation."""

    def __init__(self, workers: int = 2) -> None:
        if workers < 1:
            raise ValueError("Candle calculation workers must be positive")
        self._executor = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="candle-calculation"
        )
        self._slots = asyncio.Semaphore(workers)
        self._closed = False

    async def calculate(
        self, exchange: ExchangeId, candles: list[list[Any]]
    ) -> TypedDataFrame[KlineSchema]:
        # Acquire capacity before copying inputs or adding an executor job.
        await self._slots.acquire()
        try:
            if self._closed:
                raise RuntimeError("Candle calculation pool is closed")
            snapshot = [row.copy() for row in candles]
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, calculate_candles, exchange, snapshot
            )
        except BaseException:
            self._slots.release()
            raise

        future.add_done_callback(self._calculation_finished)
        # Cancelling the waiter cannot stop a running thread. Retain its slot
        # until the actual calculation finishes, rather than queueing more work.
        return await asyncio.shield(future)

    def _calculation_finished(
        self, future: asyncio.Future[TypedDataFrame[KlineSchema]]
    ) -> None:
        self._slots.release()
        # Retrieve failures even when the original waiter was cancelled.
        if not future.cancelled():
            future.exception()

    def close(self) -> None:
        self._closed = True
        self._executor.shutdown(wait=True, cancel_futures=True)

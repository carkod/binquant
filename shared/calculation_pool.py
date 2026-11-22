"""Bounded worker threads for calculations on privately owned input snapshots."""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from functools import partial
from typing import Any


class CalculationPool:
    """Allow at most one submitted snapshot per worker, including cancellation."""

    def __init__(self, workers: int = 2) -> None:
        if workers < 1:
            raise ValueError("Calculation workers must be positive")
        self._executor = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="strategy-calculation"
        )
        self._slots = asyncio.Semaphore(workers)
        self._closed = False

    async def calculate[T](
        self, operation: Callable[..., T], *args: Any, **kwargs: Any
    ) -> T:
        # Acquire capacity before copying inputs or adding an executor job.
        await self._slots.acquire()
        try:
            if self._closed:
                raise RuntimeError("Calculation pool is closed")
            snapshot_args, snapshot_kwargs = deepcopy((args, kwargs))
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, partial(operation, *snapshot_args, **snapshot_kwargs)
            )
        except BaseException:
            self._slots.release()
            raise

        future.add_done_callback(self._calculation_finished)
        # Cancelling the waiter cannot stop a running thread. Retain its slot
        # until the actual calculation finishes, rather than queueing more work.
        return await asyncio.shield(future)

    def _calculation_finished(self, future: asyncio.Future[Any]) -> None:
        self._slots.release()
        # Retrieve failures even when the original waiter was cancelled.
        if not future.cancelled():
            future.exception()

    def close(self) -> None:
        self._closed = True
        self._executor.shutdown(wait=True, cancel_futures=True)

from collections.abc import Callable
from time import time

from pybinbot import BinbotApi, KucoinKlineIntervals

from models.signals import SignalCandidate


class SignalCollector:
    def __init__(self):
        self.buffer: dict[str, SignalCandidate] = {}
        self.first_seen_at: int | None = None
        self.autotrade = None
        self.binbot_api = BinbotApi()
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        if self.autotrade_settings is not None:
            self.interval = self.autotrade_settings["candlestick_interval"]
        else:
            self.interval = KucoinKlineIntervals.FIFTEEN_MINUTES

    def rank(self) -> list[SignalCandidate]:
        return sorted(
            self.buffer.values(),
            key=lambda c: c.score,
            reverse=True,
        )

    async def handle(
        self,
        candidate: SignalCandidate,
        dispatch_function: Callable,
    ):
        self.autotrade = dispatch_function
        now = int(time() * 1000)

        if self.first_seen_at is None:
            self.first_seen_at = now

        self.buffer[candidate.symbol] = candidate

        max_window_ms = KucoinKlineIntervals.get_interval_ms(self.interval) * 2

        if now - self.first_seen_at >= max_window_ms:
            await self.flush()

    async def flush(self):
        ranked = self.rank()
        await self.dispatch(ranked)

        self.buffer.clear()
        self.first_seen_at = None

    async def dispatch(self, ranked: list[SignalCandidate]):
        active_bots = self.binbot_api.get_active_pairs()

        for candidate in ranked:
            if self.autotrade and candidate.symbol in active_bots:
                await self.autotrade(candidate)
            if self.autotrade and candidate.symbol in active_bots:
                await self.autotrade(candidate)

from time import time
from models.signals import SignalCandidate
from collections.abc import Callable
from pybinbot import BinbotApi


class SignalCollector:
    def __init__(self, max_window_ms=200):
        self.buffer: dict[str, SignalCandidate] = {}
        self.max_window_ms = max_window_ms
        self.first_seen_at: int | None = None
        self.autotrade = None
        self.binbot_api = BinbotApi()

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
        now = time() * 1000

        if self.first_seen_at is None:
            self.first_seen_at = int(now)

        self.buffer[candidate.symbol] = candidate

        if now - self.first_seen_at >= self.max_window_ms:
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

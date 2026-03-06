from time import time
from models.signals import SignalCandidate
from collections.abc import Callable
from pybinbot import BinbotApi, KucoinKlineIntervals, BinanceKlineIntervals
from shared.config import Config


class SignalCollector:
    def __init__(
        self,
        first_seen_at: int | None,
        interval: KucoinKlineIntervals | BinanceKlineIntervals,
    ) -> None:
        self.buffer: dict[str, SignalCandidate] = {}
        self.first_seen_at = first_seen_at
        self.autotrade: Callable
        self.send_telegram: Callable | None = None
        self.config = Config()
        self.binbot_api = BinbotApi(base_url=self.config.backend_domain)
        self.interval = interval

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
        send_telegram: Callable | None = None,
    ):
        self.autotrade = dispatch_function
        self.send_telegram = send_telegram
        now = int(time() * 1000)

        if self.first_seen_at is None:
            self.first_seen_at = now

        self.buffer[candidate.symbol] = candidate

        max_window_ms = self.interval.get_ms()
        if now - self.first_seen_at >= max_window_ms:
            await self.flush()

    async def flush(self):
        ranked = self.rank()
        await self.dispatch(ranked)

        self.buffer.clear()
        self.first_seen_at = None

    async def dispatch(self, ranked: list[SignalCandidate]):
        if not hasattr(self, "autotrade") or not callable(self.autotrade):
            raise RuntimeError("autotrade must be set to a callable before dispatching")
        active_bots = self.binbot_api.get_active_pairs()

        for candidate in ranked:
            if candidate.symbol not in active_bots:
                if self.send_telegram:
                    await self.send_telegram(candidate.msg)
                await self.autotrade(candidate)

from os import getenv
from typing import TYPE_CHECKING

from pybinbot import round_numbers, ts_to_humandate

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ApexFlow:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.latest_market_context = cls.latest_market_context
        self._last_sent_context_timestamp: int | None = None
        self._breadth_cross_tolerance = 0.05

    async def signal(
        self,
    ) -> None:

        context = self.latest_market_context

        if (
            context
            # and abs(context.advancers_ratio - context.decliners_ratio)
            # <= self._breadth_cross_tolerance
        ):
            msg = f"""
                - [{getenv("ENV")}] <strong>#market_regime_prediction</strong>
                - Timestamp: {ts_to_humandate(context.timestamp)}
                - Confidence: {round_numbers(context.confidence, 3)} (tiers: 10/20/40 fresh -> 0.35/0.65/1.0)
                - Fresh symbols: {context.fresh_count} (min 10 required)
                - Advancers ratio: {round_numbers(context.advancers_ratio, 3)} (0.50 is breadth balance)
                - Avg return: {round_numbers(context.average_return, 4)}
                - BTC regime score: {round_numbers(context.btc_regime_score, 3)} (-1 to 1, positive favors longs)
                - Long tailwind: {round_numbers(context.long_tailwind, 3)} (-1 to 1, positive is supportive)
                - Short tailwind: {round_numbers(context.short_tailwind, 3)} (-1 to 1, positive is supportive)
                - Market stress: {round_numbers(context.market_stress_score, 3)} (0 to 1, higher is worse)
            """

            await self.telegram_consumer.send_signal(msg)

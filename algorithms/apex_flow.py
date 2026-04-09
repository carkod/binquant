from typing import TYPE_CHECKING
from pybinbot import round_numbers, ts_to_humandate

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ApexFlow:
    _last_breadth_bias: str | None = None

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.config = cls.config
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.latest_market_context = cls.latest_market_context
        self._last_sent_context_timestamp: int | None = None
        self._breadth_cross_tolerance = 0.05

    def _breadth_bias(self, advancers_ratio: float) -> str | None:
        if advancers_ratio >= 0.5 + self._breadth_cross_tolerance:
            return "LONG"
        if advancers_ratio <= 0.5 - self._breadth_cross_tolerance:
            return "SHORT"
        return None

    async def signal(
        self,
    ) -> None:
        context = self.latest_market_context
        if context is None:
            return

        current_breadth_bias = self._breadth_bias(context.advancers_ratio)
        previous_breadth_bias = ApexFlow._last_breadth_bias

        if current_breadth_bias is not None:
            ApexFlow._last_breadth_bias = current_breadth_bias

        if (
            current_breadth_bias is not None
            and previous_breadth_bias is not None
            and current_breadth_bias != previous_breadth_bias
        ):
            self._last_sent_context_timestamp = context.timestamp
            msg = f"""
                - [{str(self.config.env)}] <strong>#market_regime_prediction</strong>
                - Timestamp: {ts_to_humandate(context.timestamp)}
                - Breadth flip: {previous_breadth_bias} -> {current_breadth_bias}
                - Confidence: {round_numbers(context.confidence, 3)}
                - Fresh symbols: {context.fresh_count} (min 40 required)
                - Advancers ratio: {round_numbers(context.advancers_ratio, 3)} (>= 0.55 long bias, <= 0.45 short bias)
                - Avg return: {round_numbers(context.average_return, 4)}
                - BTC regime score: {round_numbers(context.btc_regime_score, 3)} (-1 to 1, positive favors longs)
                - Long tailwind: {round_numbers(context.long_tailwind, 3)} (-1 to 1, positive is supportive)
                - Short tailwind: {round_numbers(context.short_tailwind, 3)} (-1 to 1, positive is supportive)
                - Market stress: {round_numbers(context.market_stress_score, 3)} (0 to 1, higher is worse)
            """

            await self.telegram_consumer.send_signal(msg)

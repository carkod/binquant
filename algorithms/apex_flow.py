from typing import TYPE_CHECKING

from pybinbot import round_numbers, ts_to_humandate

from market_regime.models import LiveMarketContext

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ApexFlow:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.config = cls.config
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.latest_market_context: LiveMarketContext | None = cls.latest_market_context
        self._last_sent_context_timestamp: int | None = None

    @staticmethod
    def _regime_summary(regime: str | None) -> str:
        if regime == "TREND_UP":
            return "market conditions now favor long continuation"
        if regime == "TREND_DOWN":
            return "market conditions now favor downside continuation"
        if regime == "HIGH_STRESS":
            return "market conditions have shifted into a stressed risk-off state"
        if regime == "RANGE":
            return "market conditions now favor mean-reversion and range trading"
        return "market conditions are mixed, transitional, or range-bound"

    async def signal(self) -> None:
        context = self.latest_market_context
        if context is None:
            return

        current_regime = context.market_regime
        previous_regime = context.previous_market_regime
        long_score = context.long_regime_score
        short_score = context.short_regime_score

        if (
            previous_regime is None
            or current_regime is None
            or not context.market_regime_transition
        ):
            return

        if self._last_sent_context_timestamp == context.timestamp:
            return

        self._last_sent_context_timestamp = context.timestamp
        msg = f"""
            - [{str(self.config.env)}] <strong>#market_regime_transition</strong>
            - Regime transition: {previous_regime} -> {current_regime}
            - Event: {context.market_regime_transition}
            - Interpretation: {self._regime_summary(current_regime)}
            - Timestamp: {ts_to_humandate(context.timestamp)}
            - Confidence: {round_numbers(context.confidence, 3)}
            - Transition strength: {round_numbers(context.market_regime_transition_strength, 3)}
            - Fresh symbols: {context.fresh_count} (min 40 required)
            - Advancers ratio: {round_numbers(context.advancers_ratio, 3)} (>= 0.55 long bias, <= 0.45 short bias)
            - Long regime score: {round_numbers(long_score, 3)}
            - Short regime score: {round_numbers(short_score, 3)}
            - Range regime score: {round_numbers(context.range_regime_score, 3)}
            - Stress regime score: {round_numbers(context.stress_regime_score, 3)}
            - Avg return: {round_numbers(context.average_return, 4)}
            - BTC regime score: {round_numbers(context.btc_regime_score, 3)} (-1 to 1, positive favors longs)
            - Long tailwind: {round_numbers(context.long_tailwind, 3)} (-1 to 1, positive is supportive)
            - Short tailwind: {round_numbers(context.short_tailwind, 3)} (-1 to 1, positive is supportive)
            - Market stress: {round_numbers(context.market_stress_score, 3)} (0 to 1, higher is worse)
        """

        await self.telegram_consumer.send_signal(msg)

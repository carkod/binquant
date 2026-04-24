from typing import TYPE_CHECKING

from pybinbot import round_numbers, ts_to_humandate

from market_regime.models import LiveMarketContext

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ApexFlow:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.context_evaluator = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.latest_market_context: LiveMarketContext | None = cls.latest_market_context
        self._last_sent_context_timestamp: int | None = None
        self.last_market_regime = cls.last_market_regime

    @property
    def latest_market_context(self) -> LiveMarketContext | None:
        return self.context_evaluator.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value: LiveMarketContext | None) -> None:
        self.context_evaluator.latest_market_context = value

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

        if context.market_regime_transition == self.last_market_regime:
            return

        self.last_market_regime = context.market_regime_transition
        self.context_evaluator.last_market_regime = context.market_regime_transition
        msg = f"""
            - [{str(self.config.env)}] <strong>#market_regime_transition</strong>
            - Event: {context.market_regime_transition}
            - Regime transition: {previous_regime} -> {current_regime}
            - Market regime: {current_regime}
            - Market transition: {context.market_regime_transition}
            - Interpretation: {self._regime_summary(current_regime)}
            - Timestamp: {ts_to_humandate(context.timestamp)}
            - Confidence: {round_numbers(context.confidence, 3)}
            - Transition strength: {round_numbers(context.market_regime_transition_strength, 3)}
            - Fresh symbols: {context.fresh_count}
            - Advancers ratio: {round_numbers(context.advancers_ratio, 3)}
            - Long regime score: {round_numbers(long_score, 3)}
            - Short regime score: {round_numbers(short_score, 3)}
            - Range regime score: {round_numbers(context.range_regime_score, 3)}
            - Stress regime score: {round_numbers(context.stress_regime_score, 3)}
            - Avg return: {round_numbers(context.average_return, 4)}
            - BTC regime score: {round_numbers(context.btc_regime_score, 3)}
            - Long tailwind: {round_numbers(context.long_tailwind, 3)}
            - Short tailwind: {round_numbers(context.short_tailwind, 3)}
            - Market stress: {round_numbers(context.market_stress_score, 3)}
        """

        await self.telegram_consumer.send_signal(msg)

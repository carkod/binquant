from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class MacroregimeDirectionalNotifier:
    """Notify Telegram when the shared BTC directional regime changes."""

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.context_evaluator = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.last_macroregime_directional = cls.last_macroregime_directional

    @staticmethod
    def _regime_summary(regime: str) -> str:
        if regime == "UP":
            return "BTC is moving in an efficient upward direction"
        if regime == "DOWN":
            return "BTC is moving in an efficient downward direction"
        return "BTC is not moving in a sufficiently directional trend"

    async def signal(self) -> None:
        current_regime = self.context_evaluator.macroregime_directional
        if current_regime is None:
            return

        previous_regime = self.last_macroregime_directional
        if previous_regime is None:
            self.last_macroregime_directional = current_regime
            self.context_evaluator.last_macroregime_directional = current_regime
            return

        if current_regime == previous_regime:
            return

        self.last_macroregime_directional = current_regime
        self.context_evaluator.last_macroregime_directional = current_regime
        msg = f"""
            - [{str(self.config.env)}] <strong>#macroregime_directional_transition</strong>
            - Macro directional transition: {previous_regime} -> {current_regime}
            - Interpretation: {self._regime_summary(current_regime)}
            {self.context_evaluator.context_timestamp_line()}
            {self.context_evaluator.regime_telegram_lines()}
        """

        self.telegram_consumer.dispatch_signal(msg)

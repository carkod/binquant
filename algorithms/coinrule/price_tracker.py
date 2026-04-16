import logging
import os
from typing import TYPE_CHECKING

from pybinbot import (
    HABollinguerSpread,
    Indicators,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime.regime_routing import (
    resolve_symbol_features,
)
from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.signal_context_scorer import SignalContextScorer
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class PriceTracker:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.df_5m = cls.df
        self.config = cls.config
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.exchange = cls.exchange
        self.market_breadth_data = cls.market_breadth_data
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.bot_strategy = cls.bot_strategy
        self.current_market_dominance = cls.current_market_dominance
        self.market_domination_reversal = cls.market_domination_reversal
        self.latest_market_context = cls.latest_market_context
        self._breadth_cross_tolerance = cls._breadth_cross_tolerance
        self._autotrade_stress_threshold = cls._autotrade_stress_threshold
        self.signal_context_scorer = SignalContextScorer(
            context_weight=0.35,
            risk_weight=0.35,
            support_weight=0.2,
        )

    @staticmethod
    def _has_stable_breadth(context: LiveMarketContext) -> bool:
        """
        Mean-reversion longs work best when breadth is constructive but not
        one-sided enough to imply runaway trend continuation.
        """
        breadth_is_balanced = 0.48 <= context.advancers_ratio <= 0.62
        tailwinds_are_balanced = (
            abs(context.long_tailwind - context.short_tailwind) <= 0.35
        )
        return breadth_is_balanced and tailwinds_are_balanced

    def regime_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"

        if context.regime_is_transitioning:
            return False, "market_transitioning"

        stress_threshold = min(float(self._autotrade_stress_threshold), 0.3)
        if context.market_stress_score >= stress_threshold:
            return False, "market_stress_too_high"

        breadth_not_stable_for_mean_reversion = not self._has_stable_breadth(context)
        if breadth_not_stable_for_mean_reversion:
            return False, "breadth_not_stable_for_mean_reversion"

        if context.market_regime is None:
            return False, "market_regime_unavailable"
        if context.market_regime != "RANGE":
            return False, f"market_regime_{context.market_regime.lower()}"

        if symbol_features is None:
            return False, "symbol_regime_unavailable"

        if symbol_features.micro_regime_transition in {
            "BREAKDOWN",
            "VOLATILITY_EXPANSION",
        }:
            return (
                False,
                f"symbol_transition_{symbol_features.micro_regime_transition.lower()}",
            )

        if symbol_features.micro_regime == "RANGE":
            return True, "symbol_range"

        if symbol_features.micro_regime is None:
            return False, "symbol_regime_unavailable"
        return False, f"symbol_regime_{symbol_features.micro_regime.lower()}"

    async def signal(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule price tracker algorithm
        Entry: RSI(14) < 30 AND MACD < 0 AND MFI < 20 using 5-minute candles
        BUY $30 of that coin with USDT wallet as limit order
        """
        self.df_5m = self.ti.df.copy()
        algo = "coinrule_price_tracker"

        required_cols = ["close", "rsi", "macd", "high", "low", "volume"]
        recent_window = self.df_5m[required_cols].tail(5)

        if len(self.df_5m) < 30 or recent_window.isnull().any().any():
            logging.warning(
                f"5m candles price tracker not enough data for symbol: {self.symbol}"
            )
            return

        rsi_value = float(self.df_5m["rsi"].iloc[-1])
        macd_value = float(self.df_5m["macd"].iloc[-1])
        mfi_value = Indicators.mfi(self.df_5m)

        kucoin_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[0]
        terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[1]

        if rsi_value < 30 and macd_value < 0 and mfi_value < 20:
            bot_strategy = Position.long
            autotrade = True
            context = self.latest_market_context
            local_score = (
                1.0
                + max(0.0, (30.0 - rsi_value) / 30.0) * 0.35
                + max(0.0, (20.0 - mfi_value) / 20.0) * 0.35
                + min(abs(macd_value) * 100.0, 1.0) * 0.3
            )
            ema_fast = self.df_5m["close"].ewm(span=9, adjust=False).mean().iloc[-1]
            ema_slow = self.df_5m["close"].ewm(span=21, adjust=False).mean().iloc[-1]
            trend_score = (
                float((ema_fast - ema_slow) / abs(ema_slow))
                if float(ema_slow) != 0
                else 0.0
            )

            evaluation = score_signal_candidate_with_context(
                symbol=self.symbol,
                direction="LONG",
                score=local_score,
                market_context=context,
                scorer=self.signal_context_scorer,
                local_features={
                    "trend_score": trend_score,
                },
                emit_threshold=1.0,
            )
            symbol_features = resolve_symbol_features(
                context,
                self.symbol,
            )

            if context is None:
                return

            breadth_is_stable = self._has_stable_breadth(context)
            autotrade, autotrade_route = self.regime_routing(
                context=context,
                symbol_features=symbol_features,
            )
            context_score = evaluation.context_score
            bad_followthrough = context_score.followthrough_score < -0.2
            high_risk = context_score.adverse_excursion_risk > 0.6
            low_confidence = context_score.confidence < 0.5

            if bad_followthrough or high_risk or low_confidence:
                return

            value = SignalsConsumer(
                symbol=self.symbol,
                algo=algo,
                direction="LONG",
                bot_strategy=bot_strategy,
                autotrade=autotrade,
                market_type=self.market_type,
                score=local_score,
                current_price=close_price,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {close_price}
            - RSI (14) &lt; 30: {round_numbers(rsi_value, 2)}
            - MACD &lt; 0: {round_numbers(macd_value, 6)}
            - MFI &lt; 20: {round_numbers(mfi_value, 2)}
            - Strategy: {bot_strategy.value}
            - Market regime: {context.market_regime}
            - Market transitioning: {"Yes" if context.regime_is_transitioning else "No"}
            - Market stress: {round_numbers(context.market_stress_score, 3)}
            - Advancers ratio: {round_numbers(context.advancers_ratio, 3)}
            - Long tailwind: {round_numbers(context.long_tailwind, 3)}
            - Short tailwind: {round_numbers(context.short_tailwind, 3)}
            - Breadth stable for mean-reversion: {"Yes" if breadth_is_stable else "No"}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Context confidence: {round_numbers(context_score.confidence, 2) if context_score is not None else "UNAVAILABLE"}
            - Follow-through: {round_numbers(context_score.followthrough_score, 3) if context_score is not None else "UNAVAILABLE"}
            - Risk: {round_numbers(context_score.adverse_excursion_risk, 3) if context_score is not None else "UNAVAILABLE"}
            - Adjusted score: {round_numbers(evaluation.adjusted_score, 3) if evaluation is not None else "UNAVAILABLE"}
            - Autotrade route: {autotrade_route}
            - {"Autotrade has been enabled ✅" if autotrade else "Autotrade has been disabled due to market context ❌"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

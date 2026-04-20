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

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from market_regime.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime.signal_context_scorer import SignalContextScorer
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class InversePriceTracker:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.df_5m = cls.df_5m
        self.config = cls.config
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context
        self.signal_context_scorer = SignalContextScorer(
            context_weight=0.35,
            risk_weight=0.35,
            support_weight=0.2,
        )

    @staticmethod
    def _has_bullish_transitional_market(context: LiveMarketContext) -> bool:
        if context.market_regime != "TRANSITIONAL":
            return False
        return context.long_tailwind > 0 and context.long_regime_score > max(
            context.short_regime_score,
            context.range_regime_score,
            context.stress_regime_score,
        )

    @staticmethod
    def _has_bullish_transitional_symbol(features: SymbolMarketFeatures) -> bool:
        if features.micro_regime != "TRANSITIONAL":
            return False
        return (
            features.trend_score > 0
            and features.above_ema20
            and features.relative_strength_vs_btc >= 0
        )

    def regime_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"

        if context.market_stress_score >= 0.35:
            return False, "market_stress_too_high"

        if context.market_regime is None:
            return False, "market_regime_unavailable"

        if context.market_regime == "TREND_UP":
            market_route = "market_trend_up"
        elif self._has_bullish_transitional_market(context):
            market_route = "market_transitional_bullish"
        else:
            return False, f"market_regime_{context.market_regime.lower()}"

        if symbol_features is None:
            return False, "symbol_regime_unavailable"

        if symbol_features.micro_regime == "TREND_UP":
            return True, f"{market_route}_symbol_trend_up"

        if self._has_bullish_transitional_symbol(symbol_features):
            return True, f"{market_route}_symbol_transitional_bullish"

        if symbol_features.micro_regime is None:
            return False, "symbol_regime_unavailable"
        return False, f"symbol_regime_{symbol_features.micro_regime.lower()}"

    async def signal(
        self, close_price: float, bb_high: float, bb_low: float, bb_mid: float
    ):
        self.df_5m = self.ti.df_5m.copy()
        algo = "inverse_price_tracker"

        required_cols = ["close", "rsi", "macd", "high", "low", "volume"]
        recent_window = self.df_5m[required_cols].tail(5)

        if len(self.df_5m) < 30 or recent_window.isnull().any().any():
            logging.warning(
                f"5m candles inverse price tracker not enough data for symbol: {self.symbol}"
            )
            return

        rsi_value = float(self.df_5m["rsi"].iloc[-1])
        macd_value = float(self.df_5m["macd"].iloc[-1])
        mfi_value = Indicators.mfi(self.df_5m)

        if not (rsi_value < 30 and macd_value < 0 and mfi_value < 20):
            return

        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        route_allowed, autotrade_route = self.regime_routing(
            context=context,
            symbol_features=symbol_features,
        )

        if not route_allowed or context is None or symbol_features is None:
            return

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

        context_score = evaluation.context_score
        if (
            context_score.confidence < 0.5
            or context_score.followthrough_score <= 0
            or context_score.adverse_excursion_risk > 0.55
        ):
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )
        bot_strategy = Position.long

        autotrade = False

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
            - Action: LONG ENTRY
            - Current price: {close_price}
            - RSI (14) &lt; 30: {round_numbers(rsi_value, 2)}
            - MACD &lt; 0: {round_numbers(macd_value, 6)}
            - MFI &lt; 20: {round_numbers(mfi_value, 2)}
            - Rule intent: buy an oversold pullback when the broader market and symbol context favor bullish continuation rather than balanced range mean reversion.
            - Market regime: {context.market_regime}
            - Market transition: {context.market_regime_transition if context.market_regime_transition is not None else "None"}
            - Coin regime: {symbol_features.micro_regime if symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features.micro_regime_transition is not None else "None"}
            - Context confidence: {round_numbers(context_score.confidence, 2)}
            - Follow-through: {round_numbers(context_score.followthrough_score, 3)}
            - Risk: {round_numbers(context_score.adverse_excursion_risk, 3)}
            - Adjusted score: {round_numbers(evaluation.adjusted_score, 3)}
            - Autotrade route: {autotrade_route}
            - Autotrade has been disabled for testing while inverse routing telemetry is validated 🧪
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

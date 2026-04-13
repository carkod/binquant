import logging
import os
from typing import TYPE_CHECKING

from pybinbot import (
    HABollinguerSpread,
    Indicators,
    SignalsConsumer,
    Strategy,
    round_numbers,
)

from market_regime.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime.regime_routing import (
    allows_long_autotrade,
    resolve_symbol_features,
)
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

    async def signal(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule price tracker algorithm
        Entry: RSI(14) < 30 AND MACD < 0 AND MFI < 20 using 15-minute candles
        BUY $30 of that coin with USDT wallet as limit order
        """
        self.df_5m = self.ti.df.copy()
        algo = "coinrule_price_tracker"

        if self.df_5m.isnull().values.any() or len(self.df_5m) < 30:
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
            bot_strategy = Strategy.long
            autotrade = True
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
                market_context=self.latest_market_context,
                scorer=self.signal_context_scorer,
                local_features={
                    "trend_score": trend_score,
                },
                emit_threshold=1.0,
            )
            symbol_features = resolve_symbol_features(
                self.latest_market_context,
                self.symbol,
            )

            if self.latest_market_context is not None:
                autotrade = allows_long_autotrade(
                    context=self.latest_market_context,
                    symbol=self.symbol,
                )
            else:
                return

            context_score = evaluation.context_score
            if (
                context_score.confidence >= 0.65
                and context_score.followthrough_score < -0.25
                and context_score.adverse_excursion_risk > 0.65
            ):
                return
            if (
                symbol_features is not None
                and symbol_features.micro_regime == "TREND_DOWN"
                and symbol_features.micro_regime_transition != "RECOVERY"
            ):
                return
            if not evaluation.emit:
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
            - Context available: {"Yes" if self.latest_market_context is not None else "No"}
            - Context BTC present: {"Yes" if self.latest_market_context and self.latest_market_context.btc_present else "No"}
            - Context fresh symbols: {self.latest_market_context.fresh_count if self.latest_market_context else 0}
            - Market regime: {self.latest_market_context.market_regime if self.latest_market_context else "UNAVAILABLE"}
            - Market transition: {self.latest_market_context.market_regime_transition if self.latest_market_context and self.latest_market_context.market_regime_transition is not None else "None"}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Context confidence: {round_numbers(context_score.confidence, 2)}
            - Follow-through: {round_numbers(context_score.followthrough_score, 3)}
            - Risk: {round_numbers(context_score.adverse_excursion_risk, 3)}
            - Adjusted score: {round_numbers(evaluation.adjusted_score, 3)}
            - {"Autotrade has been enabled ✅" if autotrade is True else "Autotrade has been disabled due to market context (unfavorable/unavailable) ❌"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

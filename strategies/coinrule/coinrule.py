import logging
import os
from typing import TYPE_CHECKING

from pandas import to_datetime
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    Indicators,
    MarketDominance,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.regime_routing import allows_long_autotrade, resolve_symbol_features
from market_regime.signal_context_scorer import SignalContextScorer

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class Coinrule:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
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

    def pre_process(self, df):
        df = df.copy()
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    async def twap_momentum_sniper(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule top performance rule
        uses 4 hour candles df_4h
        https://web.coinrule.com/rule/67e2b40bc6e8b64a02e2277c/draft
        """
        df = self.ti.df_5m
        df_1h = self.ti.df_1h
        if df is None or df_1h is None or df.isnull().values.any() or len(df) < 10:
            logging.warning(
                f"1h candles twap momentum not enough data for symbol: {self.symbol}"
            )
            return

        last_twap = df_1h["twap"].iloc[-1]
        price_decrease = df_1h["close"].iloc[-1] - (
            df_1h["close"].iloc[-2] / df_1h["close"].iloc[-1]
        )

        if last_twap > close_price and price_decrease > -0.05:
            algo = "coinrule_twap_momentum_sniper"
            autotrade = False
            context = self.latest_market_context
            symbol_features = resolve_symbol_features(
                context=context, symbol=self.symbol
            )
            action = (
                f"{self.bot_strategy.value.upper()} ENTRY"
                if self.bot_strategy is not None
                else "ENTRY"
            )

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: {action}
            - Current price: {close_price}
            - Strategy: {self.bot_strategy.value}
            - Rule intent: Enter when TWAP stays above price without a sharp recent selloff
            - Market regime: {context.market_regime if context is not None and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - TWAP (> current price): {round_numbers(last_twap)}
            - Autotrade route: manual_only
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=close_price,
                bot_params=BotBase(
                    pair=self.symbol,
                    name=algo,
                    position=self.bot_strategy,
                    market_type=self.market_type,
                ),
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

    async def supertrend_swing_reversal(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule top performance rule
        https://web.coinrule.com/rule/67c8bf4bdb949c69ab4200b3/draft

        Uses 1 hour candles df
        """
        df = self.ti.df_5m
        if df is None or df.isnull().values.any() or df.size == 0:
            logging.warning("1h candles supertrend have null values")
            return

        df = self.pre_process(df)

        # Reuse shared Supertrend (period adjusted to 10 to match strategy)
        Indicators.set_supertrend(df, multiplier=3.0)

        adp_diff = (
            self.market_breadth_data["adp"][-1] - self.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.market_breadth_data["adp"][-2] - self.market_breadth_data["adp"][-3]
        )

        if (
            bool(df["supertrend"].iloc[-1])
            and df["rsi"].iloc[-1] < 30
            and df["number_of_trades"].iloc[-1] > 5
            and adp_diff > 0
            and adp_diff_prev > 0
            and self.current_market_dominance == MarketDominance.LOSERS
        ):
            algo = "coinrule_supertrend_swing_reversal"
            bot_strategy = Position.long
            autotrade = True
            context = self.latest_market_context
            if context is not None:
                autotrade = allows_long_autotrade(
                    context=context,
                    symbol=self.symbol,
                )
            symbol_features = resolve_symbol_features(
                context=context, symbol=self.symbol
            )
            last_timestamp = (
                to_datetime(df["close_time"][-1:], unit="ms")
                .dt.strftime("%Y-%m-%d %H:%M")
                .iloc[0]
            )

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {close_price}
            - Strategy: {bot_strategy.value}
            - Rule intent: BUY a supertrend swing reversal after oversold conditions and improving breadth
            - Market regime: {context.market_regime if context is not None and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Candle time: {last_timestamp}
            - Number of trades: {df["number_of_trades"].iloc[-1]}
            - RSI smaller than 30: {df["rsi"].iloc[-1]}
            - Autotrade route: {"long_autotrade_allowed" if autotrade else "manual_only"}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=close_price,
                bot_params=BotBase(
                    pair=self.symbol,
                    name=algo,
                    position=bot_strategy,
                    market_type=self.market_type,
                ),
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

    async def buy_low_sell_high(
        self,
        close_price,
        rsi,
        ma_25,
        bb_high,
        bb_mid,
        bb_low,
    ):
        """
        Coinrule top performance rule
        https://web.coinrule.com/share-rule/Multi-Time-Frame-Buy-Low-Sell-High-Short-term-8f02df
        """
        bot_strategy = self.bot_strategy

        if rsi < 35 and close_price > ma_25 and self.market_domination_reversal:
            algo = "coinrule_buy_low_sell_high"

            bot_strategy = Position.long
            autotrade = False
            context = self.latest_market_context
            symbol_features = resolve_symbol_features(
                context=context, symbol=self.symbol
            )
            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {close_price}
            - Strategy: {bot_strategy.value}
            - Rule intent: BUY low inside a short-term reversal while price stays above the 25-period average
            - Market regime: {context.market_regime if context is not None and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Bollinger bands spread: {(bb_high - bb_low) / bb_high}
            - Reversal state: {"Positive" if self.market_domination_reversal else "Negative"}
            - Autotrade route: manual_only
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=close_price,
                bot_params=BotBase(
                    pair=self.symbol,
                    name=algo,
                    position=bot_strategy,
                    market_type=self.market_type,
                ),
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

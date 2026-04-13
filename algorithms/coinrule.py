import logging
import os
from typing import TYPE_CHECKING
from pandas import to_datetime
from pybinbot import (
    HABollinguerSpread,
    MarketDominance,
    SignalsConsumer,
    Strategy,
    round_numbers,
    Indicators,
)
from market_regime_prediction.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime_prediction.signal_context_scorer import SignalContextScorer
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class GridTrading:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.config = cls.config
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context

    async def signal(
        self,
        current_price,
        bb_high,
        bb_mid,
        bb_low,
    ):
        """
        Coinrule-inspired grid trading rule for range-bound markets.

        Best when the market is chopping sideways and price repeatedly
        oscillates around a stable band. The setup buys oversold dips inside
        the range and proposes scaling out 10% every +2% move while keeping
        residual exposure open.
        """
        context = self.latest_market_context
        if context is not None:
            is_vertical_market = context.market_stress_score >= 0.35
            is_downtrend_market = (
                context.advancers_ratio <= 0.45 or context.long_tailwind <= 0
            )
            if is_vertical_market or is_downtrend_market:
                return

        df_15m = self.ti.df_15m
        if df_15m is None or len(df_15m) < 48:
            logging.warning(
                f"15m candles grid trading not enough data for symbol: {self.symbol}"
            )
            return

        required_cols = ["high", "low", "close", "rsi"]
        recent_window = df_15m.tail(48)
        if recent_window[required_cols].isnull().any().any():
            return

        recent_high = float(recent_window["high"].max())
        recent_low = float(recent_window["low"].min())
        recent_close_base = float(recent_window["close"].iloc[0])
        range_width = (
            (recent_high - recent_low) / recent_low if recent_low != 0 else 0.0
        )
        range_drift = (
            abs((current_price - recent_close_base) / recent_close_base)
            if recent_close_base != 0
            else 0.0
        )
        rsi_value = float(df_15m["rsi"].iloc[-1])
        bb_width = ((bb_high - bb_low) / bb_mid) if bb_mid else 0.0
        lower_band_position = (
            (current_price - bb_low) / (bb_high - bb_low) if bb_high != bb_low else 0.5
        )

        is_range_market = (
            range_width >= 0.015 and range_drift <= 0.03 and 0.01 <= bb_width <= 0.08
        )
        buy_zone = (
            rsi_value < 35 and current_price <= bb_mid and lower_band_position <= 0.4
        )
        reduce_zone = rsi_value > 65

        if not is_range_market or not buy_zone:
            return

        algo = "coinrule_grid_trading"
        bot_strategy = Strategy.long
        autotrade = False

        kucoin_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[0]
        terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[1]

        msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {current_price}
            - Strategy: {bot_strategy.value}
            - Market mode: Sideways / range-bound
            - RSI buy zone (&lt; 35): {round_numbers(rsi_value, 2)}
            - Range width (24 candles): {round_numbers(range_width * 100, 2)}%
            - Range drift (24 candles): {round_numbers(range_drift * 100, 2)}%
            - Bollinger width: {round_numbers(bb_width * 100, 2)}%
            - Grid action: Buy ladder into weakness below the mid-band
            - Scale-out plan: Sell 10% every +2% move while keeping position open
            - Reduce exposure trigger: {"Active" if reduce_zone else "Stand by until RSI > 65"}
            - {"Autotrade is disabled for testing" if not autotrade else "Autotrade is enabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            symbol=self.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            market_type=self.market_type,
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)


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
        df = self.ti.df
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

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {close_price}
            - Strategy: {self.bot_strategy.value}
            - TWAP (> current price): {round_numbers(last_twap)}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=close_price,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=self.bot_strategy,
                market_type=self.market_type,
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
        df = self.ti.df
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
            bot_strategy = Strategy.long
            autotrade = True
            context = self.latest_market_context
            if context is not None:
                if context.market_stress_score >= self._autotrade_stress_threshold:
                    autotrade = False
                elif context.advancers_ratio >= 0.5 + self._breadth_cross_tolerance:
                    autotrade = bot_strategy == Strategy.long
                elif context.advancers_ratio <= 0.5 - self._breadth_cross_tolerance:
                    autotrade = bot_strategy == Strategy.margin_short
            last_timestamp = (
                to_datetime(df["close_time"][-1:], unit="ms")
                .dt.strftime("%Y-%m-%d %H:%M")
                .iloc[0]
            )

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - 📅 {last_timestamp}
            - Number of trades: {df["number_of_trades"].iloc[-1]}
            - Current price: {close_price}
            - Strategy: {bot_strategy.value}
            - RSI smaller than 30: {df["rsi"].iloc[-1]}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=close_price,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                market_type=self.market_type,
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

            bot_strategy = Strategy.long
            autotrade = False
            msg = f"""
            - [{os.getenv("ENV")}] <strong>{algo} #algorithm</strong> #{self.symbol}
            - Current price: {close_price}
            - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
            - Strategy: {bot_strategy.value}
            - Reversal? {"No reversal" if not self.market_domination_reversal else "Positive" if self.market_domination_reversal else "Negative"}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - https://www.binance.com/en/trade/{self.symbol}
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=close_price,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                market_type=self.market_type,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass


class PriceTracker:
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

    async def signal(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule price tracker algorithm
        Entry: RSI(14) < 30 AND MACD < 0 AND MFI < 20 using 15-minute candles
        BUY $30 of that coin with USDT wallet as limit order
        """
        df_15m = self.ti.df_15m.copy()
        algo = "coinrule_price_tracker"

        if df_15m.isnull().values.any() or len(df_15m) < 30:
            logging.warning(
                f"15m candles price tracker not enough data for symbol: {self.symbol}"
            )
            return

        rsi_value = float(df_15m["rsi"].iloc[-1])
        macd_value = float(df_15m["macd"].iloc[-1])
        mfi_value = Indicators.mfi(df_15m)

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
            ema_fast = df_15m["close"].ewm(span=9, adjust=False).mean().iloc[-1]
            ema_slow = df_15m["close"].ewm(span=21, adjust=False).mean().iloc[-1]
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

            if self.latest_market_context is not None:
                if self.latest_market_context.market_stress_score >= 0.35:
                    autotrade = False
                elif self.latest_market_context.advancers_ratio >= 0.55:
                    autotrade = bot_strategy == Strategy.long
                elif self.latest_market_context.advancers_ratio <= 0.45:
                    # liquidation sweep pump is mostly designed as a long bot
                    autotrade = False
            else:
                return

            context_score = evaluation.context_score
            if (
                context_score.confidence >= 0.65
                and context_score.followthrough_score < -0.25
                and context_score.adverse_excursion_risk > 0.65
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

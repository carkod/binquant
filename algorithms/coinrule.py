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

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class Coinrule:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.df = cls.df
        self.df_1h = cls.df_1h
        self.df_5m = cls.df_5m
        self.market_breadth_data = cls.market_breadth_data
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.bot_strategy = cls.bot_strategy
        self.current_market_dominance = cls.current_market_dominance
        self.market_domination_reversal = cls.market_domination_reversal

    def pre_process(self):
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    async def twap_momentum_sniper(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule top performance rule
        uses 4 hour candles df_4h
        https://web.coinrule.com/rule/67e2b40bc6e8b64a02e2277c/draft
        """
        if self.df.isnull().values.any() or len(self.df) < 10:
            logging.warning(
                f"1h candles twap momentum not enough data for symbol: {self.symbol}"
            )
            return

        last_twap = self.df_1h["twap"].iloc[-1]
        price_decrease = self.df_1h["close"].iloc[-1] - (
            self.df_1h["close"].iloc[-2] / self.df_1h["close"].iloc[-1]
        )

        if last_twap > close_price and price_decrease > -0.05:
            algo = "coinrule_twap_momentum_sniper"

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {close_price}
            - Strategy: {self.bot_strategy.value}
            - TWAP (> current price): {round_numbers(last_twap)}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=self.bot_strategy,
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
        if self.df.isnull().values.any() or self.df.size == 0:
            logging.warning("1h candles supertrend have null values")
            return

        self.pre_process()

        # Reuse shared Supertrend (period adjusted to 10 to match strategy)
        Indicators.set_supertrend(self.df, multiplier=3.0)

        adp_diff = (
            self.market_breadth_data["adp"][-1] - self.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.market_breadth_data["adp"][-2] - self.market_breadth_data["adp"][-3]
        )

        if (
            bool(self.df["supertrend"].iloc[-1])
            and self.df["rsi"].iloc[-1] < 30
            and self.df["number_of_trades"].iloc[-1] > 5
            and adp_diff > 0
            and adp_diff_prev > 0
            and self.current_market_dominance == MarketDominance.LOSERS
        ):
            algo = "coinrule_supertrend_swing_reversal"
            bot_strategy = Strategy.long
            last_timestamp = (
                to_datetime(self.df["close_time"][-1:], unit="ms")
                .dt.strftime("%Y-%m-%d %H:%M")
                .iloc[0]
            )

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - 📅 {last_timestamp}
            - Number of trades: {self.df["number_of_trades"].iloc[-1]}
            - Current price: {close_price}
            - Strategy: {bot_strategy.value}
            - RSI smaller than 30: {self.df["rsi"].iloc[-1]}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=True,
                current_price=close_price,
                msg=msg,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
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
            msg = f"""
            - [{os.getenv("ENV")}] <strong>{algo} #algorithm</strong> #{self.symbol}
            - Current price: {close_price}
            - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
            - Strategy: {bot_strategy.value}
            - Reversal? {"No reversal" if not self.market_domination_reversal else "Positive" if self.market_domination_reversal else "Negative"}
            - https://www.binance.com/en/trade/{self.symbol}
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

    @staticmethod
    def _compute_mfi(df, window: int = 14) -> float:
        """
        Money Flow Index (MFI) using the last `window` candles.

        Requires 'high', 'low', 'close', 'volume' columns.
        Returns the most recent MFI value (0–100).
        When negative money flow is zero (pure uptrend), MFI is 100.
        """
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        raw_money_flow = typical_price * df["volume"]

        tp_change = typical_price.diff()
        positive_flow = raw_money_flow.where(tp_change > 0, 0.0)
        negative_flow = raw_money_flow.where(tp_change < 0, 0.0)

        positive_sum = positive_flow.rolling(window).sum()
        negative_sum = negative_flow.rolling(window).sum()

        # When negative_sum is 0 (pure uptrend), MFI = 100
        money_ratio = positive_sum / negative_sum.replace(0, float("nan"))
        raw_mfi = 100 - (100 / (1 + money_ratio))
        mfi = raw_mfi.fillna(100.0)
        return float(mfi.iloc[-1])

    async def price_tracker(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule price tracker algorithm
        Entry: RSI(14) < 30 AND MACD < 0 AND MFI < 20 using 5-minute candles
        BUY $30 of that coin with USDT wallet as limit order
        """
        df_5m = self.df_5m.copy()

        if df_5m.isnull().values.any() or len(df_5m) < 30:
            logging.warning(
                f"5m candles price tracker not enough data for symbol: {self.symbol}"
            )
            return

        df_5m.dropna(inplace=True)
        df_5m.reset_index(drop=True, inplace=True)

        df_5m = Indicators.rsi(df=df_5m)
        df_5m = Indicators.macd(df_5m)

        rsi_value = float(df_5m["rsi"].iloc[-1])
        macd_value = float(df_5m["macd"].iloc[-1])
        mfi_value = self._compute_mfi(df_5m)

        if rsi_value < 30 and macd_value < 0 and mfi_value < 20:
            algo = "coinrule_price_tracker"
            bot_strategy = Strategy.long

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {close_price}
            - RSI (14) < 30: {round_numbers(rsi_value, 2)}
            - MACD < 0: {round_numbers(macd_value, 6)}
            - MFI < 20: {round_numbers(mfi_value, 2)}
            - Strategy: {bot_strategy.value}
            - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=True,
                current_price=close_price,
                msg=msg,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.telegram_consumer.send_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        pass

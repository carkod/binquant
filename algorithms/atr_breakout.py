import logging
import os
from typing import TYPE_CHECKING

import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class ATRBreakout:
    def __init__(self, cls: "TechnicalIndicators") -> None:
        """
        Calculate the Average True Range (ATR) indicator.
        ATR is a measure of volatility.

        Choice of window
        Window	Behavior        When to use
        3	    Very sensitive	Scalping, volatile coins
        5	    Fast & balanced	Intraday breakouts (like your case)
        10–14	Smoothed	    Swing setups, reduce noise
        20+	    Very stable	    Long-term trends only
        """
        self.ti = cls

    def preprocess(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        df = df.copy()

        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time", inplace=True)
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)

        # ATR breakout logic
        tr = pd.concat(
            [
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)

        df["amplitude"] = df["high"] - df["low"]

        df["ATR"] = tr.rolling(window=30, min_periods=20).mean()
        df["rolling_high"] = df["high"].rolling(window=30).max().shift(1)
        df["breakout_strength"] = (df["close"] - df["rolling_high"]) / df["ATR"]
        df["ATR_breakout"] = (df["close"] > (df["rolling_high"] + 1.1 * df["ATR"])) & (
            df["breakout_strength"] > 0.05
        )
        self.df = df

    async def reverse_atr_breakout(self, bb_high, bb_low, bb_mid):
        """
        Reverse atr_breakout
        When market is bearish, most prices decreasing,
        assets that have a negative correlation to BTC can potentially go up

        """
        self.preprocess(self.ti.df)

        if "ATR_breakout" not in self.df.columns:
            logging.error(f"ATP breakout not enough data for symbol: {self.ti.symbol}")
            return

        green_candle = self.df["close"] > self.df["open"]

        if (
            (
                self.df["ATR_breakout"].iloc[-1]
                or self.df["ATR_breakout"].iloc[-2]
                or self.df["ATR_breakout"].iloc[-3]
            )
            and green_candle.iloc[-1]
            # and volume_confirmation.iloc[-1]
            and self.ti.btc_correlation < 0
        ):
            algo = "reverse_atr_breakout"
            close_price = self.df["close"].iloc[-1]

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {close_price}
            - Strategy: {self.ti.bot_strategy.value}
            - BTC correlation: {round_numbers(self.ti.btc_correlation)}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=self.ti.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.ti.telegram_consumer.send_signal(value)
            # self.ti.at_consumer.process_autotrade_restrictions(value)

    async def atr_breakout(self, bb_high, bb_low, bb_mid):
        """
        ATR breakout detection algorithm based on chatGPT

        Detect breakout: price close above previous high AND ATR spike
        """

        self.preprocess(self.ti.df)

        if "ATR_breakout" not in self.df.columns:
            logging.error(f"ATP breakout not enough data for symbol: {self.ti.symbol}")
            return

        green_candle = self.df["close"] > self.df["open"]
        volume_confirmation = self.df["volume"] > self.df["volume"].rolling(20).mean()

        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )

        if (
            (self.df["ATR_breakout"].iloc[-1] or self.df["ATR_breakout"].iloc[-2])
            and green_candle.iloc[-1]
            and volume_confirmation.iloc[-1]
            # and cls.current_market_dominance == MarketDominance.LOSERS
            # check market is bullish. we don't want to trade when all assets are uptrend
            # because the potential of growth is low, market is already mature
            # still want to get in when there is a trend (positive ADP)
            and adp_diff > 0
            and adp_diff_prev > 0
        ):
            algo = "atr_breakout"
            close_price = self.df["close"].iloc[-1]

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {close_price}
            - Strategy: {self.ti.bot_strategy.value}
            - BTC correlation: {round_numbers(self.ti.btc_correlation)}
            - Autotrade?: {"No"}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=self.ti.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.ti.telegram_consumer.send_signal(value)
            await self.ti.at_consumer.process_autotrade_restrictions(value)

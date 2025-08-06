import logging
import os
from typing import TYPE_CHECKING

import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class ATRBreakout:
    def __init__(self, origin_df: pd.DataFrame):
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

        if origin_df.empty:
            return

        df = origin_df.copy()

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

    async def reverse_atr_breakout(
        self, cls: "TechnicalIndicators", bb_high, bb_low, bb_mid
    ):
        """
        Reverse atr_breakout
        When market is bearish, most prices decreasing,
        assets that have a negative correlation to BTC can potentially go up

        """

        if "ATR_breakout" not in self.df:
            logging.error(f"ATP breakout not enough data for symbol: {cls.symbol}")
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
            and cls.btc_correlation < 0
        ):
            algo = "reverse_atr_breakout"
            close_price = self.df["close"].iloc[-1]

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
            - Current price: {close_price}
            - Strategy: {cls.bot_strategy.value}
            - BTC correlation: {round_numbers(cls.btc_correlation)}
            - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=cls.symbol,
                algo=algo,
                bot_strategy=cls.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await cls.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

    async def atr_breakout(self, cls: "TechnicalIndicators", bb_high, bb_low, bb_mid):
        """
        ATR breakout detection algorithm based on chatGPT

        Detect breakout: price close above previous high AND ATR spike
        """

        if "ATR_breakout" not in self.df:
            logging.error(f"ATP breakout not enough data for symbol: {cls.symbol}")
            return

        green_candle = self.df["close"] > self.df["open"]
        volume_confirmation = self.df["volume"] > self.df["volume"].rolling(20).mean()

        adp_diff = (
            cls.market_breadth_data["adp"][-1] - cls.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            cls.market_breadth_data["adp"][-2] - cls.market_breadth_data["adp"][-3]
        )

        print(f"ATR_breakout: {self.df['ATR_breakout'].iloc[-1]}")

        if (
            (
                self.df["ATR_breakout"].iloc[-1]
                or self.df["ATR_breakout"].iloc[-2]
                or self.df["ATR_breakout"].iloc[-3]
            )
            and green_candle.iloc[-1]
            and volume_confirmation.iloc[-1]
            and cls.current_market_dominance == MarketDominance.LOSERS
            # check market is bullish. we don't want to trade when all assets are uptrend
            # because the potential of growth is low, market is already mature
            # still want to get in when there is a trend (positive ADP)
            and adp_diff > 0
            and adp_diff_prev > 0
        ):
            algo = "atr_breakout"
            close_price = self.df["close"].iloc[-1]

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
            - Current price: {close_price}
            - Strategy: {cls.bot_strategy.value}
            - BTC correlation: {round_numbers(cls.btc_correlation)}
            - Autotrade?: {"No"}
            - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=cls.symbol,
                algo=algo,
                bot_strategy=cls.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await cls.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

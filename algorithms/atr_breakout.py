import logging
import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import MarketDominance
from shared.indicators import Indicators
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class ATRBreakout:
    def __init__(self, cls: "CryptoAnalytics") -> None:
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
        # Every df should be isolated to avoid affecting the parsing of the dataframe
        #  of other algos
        df = cls.df.copy()
        self.df = Indicators.atr(df=df, window=30, min_periods=20)

    async def reverse_atr_breakout(self, bb_high, bb_low, bb_mid):
        """
        Reverse atr_breakout
        When market is bearish, most prices decreasing,
        assets that have a negative correlation to BTC can potentially go up

        """

        if "ATR_breakout" not in self.df.columns:
            logging.error(f"ATP breakout not enough data for symbol: {self.ti.symbol}")
            return

        green_candle = self.df["close"].iloc[-1] > self.df["open"].iloc[-1]

        if (
            (
                self.df["ATR_breakout"].iloc[-1]
                or self.df["ATR_breakout"].iloc[-2]
                or self.df["ATR_breakout"].iloc[-3]
            )
            and green_candle.iloc[-1]
            # and volume_confirmation.iloc[-1]
            and self.ti.btc_correlation < 0
            and self.ti.current_market_dominance == MarketDominance.LOSERS
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

            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

    async def atr_breakout(self, bb_high, bb_low, bb_mid):
        """
        ATR breakout detection algorithm based on chatGPT

        Detect breakout: price close above previous high AND ATR spike
        """
        if "ATR_breakout" not in self.df.columns:
            logging.error(f"ATP breakout not enough data for symbol: {self.ti.symbol}")
            return

        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )

        if (
            (
                bool(self.df["ATR_breakout"].iloc[-1])
                or bool(self.df["ATR_breakout"].iloc[-2])
                or bool(self.df["ATR_breakout"].iloc[-3])
            )
            # and self.ti.current_market_dominance == MarketDominance.LOSERS
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

            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

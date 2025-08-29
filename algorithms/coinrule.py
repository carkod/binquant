import logging
import os
from typing import TYPE_CHECKING

import pandas

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class Coinrule:
    def __init__(self, cls: "CryptoAnalytics") -> None:
        self.cls = cls

    async def twap_momentum_sniper(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule top performance rule
        uses 4 hour candles df_4h
        https://web.coinrule.com/rule/67e2b40bc6e8b64a02e2277c/draft
        """
        if self.cls.df_1h.isnull().values.any() or len(self.cls.df_1h) < 10:
            logging.warning(
                f"1h candles twap momentum not enough data for symbol: {self.cls.symbol}"
            )
            return

        last_twap = self.cls.df_1h["twap"].iloc[-1]
        price_decrease = self.cls.df_1h["close"].iloc[-1] - (
            self.cls.df_1h["close"].iloc[-2] / self.cls.df_1h["close"].iloc[-1]
        )

        if last_twap > close_price and price_decrease > -0.05:
            algo = "coinrule_twap_momentum_sniper"

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.cls.symbol}
            - Current price: {close_price}
            - Strategy: {self.cls.bot_strategy.value}
            - TWAP (> current price): {round_numbers(last_twap)}
            - <a href='https://www.binance.com/en/trade/{self.cls.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.cls.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.cls.symbol,
                algo=algo,
                bot_strategy=self.cls.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.cls.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

        pass

    async def supertrend_swing_reversal(self, close_price, bb_high, bb_low, bb_mid):
        """
        Coinrule top performance rule
        https://web.coinrule.com/rule/67c8bf4bdb949c69ab4200b3/draft

        Uses 1 hour candles df
        """
        if self.cls.df.isnull().values.any() or self.cls.df.size == 0:
            logging.warning("1h candles supertrend have null values")
            return

        hl2 = (self.cls.df["high"] + self.cls.df["low"]) / 2
        period = 10
        multiplier = 3.0

        # True Range (TR)
        previous_close = self.cls.df["close"].shift(1)
        high_low = self.cls.df["high"] - self.cls.df["low"]
        high_pc = abs(self.cls.df["high"] - previous_close)
        low_pc = abs(self.cls.df["low"] - previous_close)
        tr = pandas.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)

        # Average True Range (ATR)
        self.cls.df["atr"] = tr.rolling(window=period).mean()

        # Bands
        self.cls.df["upperband"] = hl2 + (multiplier * self.cls.df["atr"])
        self.cls.df["lowerband"] = hl2 - (multiplier * self.cls.df["atr"])

        supertrend = []

        for i in range(period, len(self.cls.df)):
            if self.cls.df["close"].iloc[i - 1] > self.cls.df["upperband"].iloc[i - 1]:
                self.cls.df.at[i, "upperband"] = max(
                    self.cls.df["upperband"].iloc[i],
                    self.cls.df["upperband"].iloc[i - 1],
                )
            else:
                self.cls.df.at[i, "upperband"] = self.cls.df["upperband"].iloc[i]

            if self.cls.df["close"].iloc[i - 1] < self.cls.df["lowerband"].iloc[i - 1]:
                self.cls.df.at[i, "lowerband"] = min(
                    self.cls.df["lowerband"].iloc[i],
                    self.cls.df["lowerband"].iloc[i - 1],
                )
            else:
                self.cls.df.at[i, "lowerband"] = self.cls.df["lowerband"].iloc[i]

            # Determine trend direction
            if self.cls.df["close"].iloc[i] > self.cls.df["upperband"].iloc[i - 1]:
                supertrend.append(True)
            elif self.cls.df["close"].iloc[i] < self.cls.df["lowerband"].iloc[i - 1]:
                supertrend.append(False)

        if (
            len(supertrend) > 0
            and supertrend[-1]
            and self.cls.df["rsi"].iloc[-1] < 30
            # Long position bots
            and self.cls.market_breadth_data["adp"][-1] > 0
            and self.cls.market_breadth_data["adp"][-2] > 0
            and self.cls.market_breadth_data["adp"][-3] > 0
        ):
            algo = "coinrule_supertrend_swing_reversal"
            bb_high, bb_mid, bb_low = self.cls.bb_spreads()
            bot_strategy = Strategy.long

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.cls.symbol}
            - Current price: {close_price}
            - Strategy: {bot_strategy.value}
            - RSI smaller than 30: {self.cls.df["rsi"].iloc[-1]}
            - <a href='https://www.binance.com/en/trade/{self.cls.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.cls.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.cls.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.cls.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

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
        bot_strategy = self.cls.bot_strategy

        if rsi < 35 and close_price > ma_25 and self.cls.market_domination_reversal:
            algo = "coinrule_buy_low_sell_high"

            bot_strategy = Strategy.long
            msg = f"""
            - [{os.getenv("ENV")}] <strong>{algo} #algorithm</strong> #{self.cls.symbol}
            - Current price: {close_price}
            - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
            - Strategy: {bot_strategy.value}
            - Reversal? {"No reversal" if not self.cls.market_domination_reversal else "Positive" if self.cls.market_domination_reversal else "Negative"}
            - https://www.binance.com/en/trade/{self.cls.symbol}
            - <a href='http://terminal.binbot.in/bots/new/{self.cls.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.cls.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.cls.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

        pass

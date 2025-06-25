import logging
import os
from typing import TYPE_CHECKING

import pandas

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


async def twap_momentum_sniper(
    cls: "TechnicalIndicators", close_price, bb_high, bb_low, bb_mid
):
    """
    Coinrule top performance rule
    uses 4 hour candles df_4h
    https://web.coinrule.com/rule/67e2b40bc6e8b64a02e2277c/draft
    """
    if cls.df_1h.isnull().values.any() or len(cls.df_1h) < 10:
        logging.warning(
            f"1h candles twap momentum not enough data for symbol: {cls.symbol}"
        )
        return

    last_twap = cls.df_1h["twap"].iloc[-1]
    price_decrease = cls.df_1h["close"].iloc[-1] - (
        cls.df_1h["close"].iloc[-2] / cls.df_1h["close"].iloc[-1]
    )

    if last_twap > close_price and price_decrease > -0.05:
        algo = "coinrule_twap_momentum_sniper"

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {cls.bot_strategy.value}
        - TWAP (> current price): {round_numbers(last_twap)}
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

    pass


async def supertrend_swing_reversal(
    cls: "TechnicalIndicators", close_price, bb_high, bb_low, bb_mid
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/rule/67c8bf4bdb949c69ab4200b3/draft

    Uses 1 hour candles df_1h
    """
    if cls.df_1h.isnull().values.any() or cls.df_1h.size == 0:
        logging.warning("1h candles supertrend have null values")
        return

    hl2 = (cls.df_1h["high"] + cls.df_1h["low"]) / 2
    period = 10
    multiplier = 3.0

    # True Range (TR)
    previous_close = cls.df_1h["close"].shift(1)
    high_low = cls.df_1h["high"] - cls.df_1h["low"]
    high_pc = abs(cls.df_1h["high"] - previous_close)
    low_pc = abs(cls.df_1h["low"] - previous_close)
    tr = pandas.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)

    # Average True Range (ATR)
    cls.df_1h["atr"] = tr.rolling(window=period).mean()

    # Bands
    cls.df_1h["upperband"] = hl2 + (multiplier * cls.df_1h["atr"])
    cls.df_1h["lowerband"] = hl2 - (multiplier * cls.df_1h["atr"])

    supertrend = []

    for i in range(period, len(cls.df_1h)):
        if cls.df_1h["close"].iloc[i - 1] > cls.df_1h["upperband"].iloc[i - 1]:
            cls.df_1h.at[i, "upperband"] = max(
                cls.df_1h["upperband"].iloc[i], cls.df_1h["upperband"].iloc[i - 1]
            )
        else:
            cls.df_1h.at[i, "upperband"] = cls.df_1h["upperband"].iloc[i]

        if cls.df_1h["close"].iloc[i - 1] < cls.df_1h["lowerband"].iloc[i - 1]:
            cls.df_1h.at[i, "lowerband"] = min(
                cls.df_1h["lowerband"].iloc[i], cls.df_1h["lowerband"].iloc[i - 1]
            )
        else:
            cls.df_1h.at[i, "lowerband"] = cls.df_1h["lowerband"].iloc[i]

        # Determine trend direction
        if cls.df_1h["close"].iloc[i] > cls.df_1h["upperband"].iloc[i - 1]:
            supertrend.append(True)
        elif cls.df_1h["close"].iloc[i] < cls.df_1h["lowerband"].iloc[i - 1]:
            supertrend.append(False)

    if (
        len(supertrend) > 0
        and supertrend[-1]
        and cls.df_1h["rsi"].iloc[-1] < 30
        # Long position bots
        and cls.market_breadth_data["adp"][-1] > 0
        and cls.market_breadth_data["adp"][-2] > 0
        and cls.market_breadth_data["adp"][-3] > 0
    ):
        algo = "coinrule_supertrend_swing_reversal"
        bb_high, bb_mid, bb_low = cls.bb_spreads()
        bot_strategy = Strategy.long

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {bot_strategy.value}
        - RSI smaller than 30: {cls.df["rsi"].iloc[-1]}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=False,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        )

    pass


async def buy_low_sell_high(
    cls: "TechnicalIndicators",
    close_price,
    rsi,
    ma_25,
    volatility,
    bb_high,
    bb_mid,
    bb_low,
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Multi-Time-Frame-Buy-Low-Sell-High-Short-term-8f02df
    """
    volatility = round_numbers(volatility, 6)
    bot_strategy = cls.bot_strategy

    if (
        rsi < 35
        and close_price > ma_25
        and volatility > 0.01
        and cls.market_domination_reversal
    ):
        algo = "coinrule_buy_low_sell_high"
        volatility = round_numbers(volatility, 6)

        bot_strategy = Strategy.long
        msg = f"""
        - [{os.getenv("ENV")}] <strong>{algo} #algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
        - Strategy: {bot_strategy.value}
        - Reversal? {"No reversal" if not cls.market_domination_reversal else "Positive" if cls.market_domination_reversal else "Negative"}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=False,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        )

    pass

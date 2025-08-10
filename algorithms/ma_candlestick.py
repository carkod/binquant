import logging
import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


async def reverse_atr_breakout(cls: "TechnicalIndicators", bb_high, bb_low, bb_mid):
    """
    Reverse atr_breakout
    When market is bearish, most prices decreasing,
    assets that have a negative correlation to BTC can potentially go up

    """

    if "ATR_breakout" not in cls.df:
        logging.error(f"ATP breakout not enough data for symbol: {cls.symbol}")
        return

    green_candle = cls.df["close"] > cls.df["open"]

    adp_diff = cls.market_breadth_data["adp"][-1] - cls.market_breadth_data["adp"][-2]
    adp_diff_prev = (
        cls.market_breadth_data["adp"][-2] - cls.market_breadth_data["adp"][-3]
    )

    if (
        cls.df["ATR_breakout"].iloc[-1]
        and green_candle.iloc[-1]
        # and volume_confirmation.iloc[-1]
        and cls.btc_correlation < 0
        # because the potential of growth is low, market is already mature
        # still want to get in when there is a trend (positive ADP)
        and cls.market_breadth_data["adp"][-1] > 0
        and adp_diff > 0
        and adp_diff_prev > 0
    ):
        algo = "reverse_atr_breakout"
        close_price = cls.df["close"].iloc[-1]

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


async def atr_breakout(cls: "TechnicalIndicators", bb_high, bb_low, bb_mid):
    """
    ATR breakout detection algorithm based on chatGPT

    Detect breakout: price close above previous high AND ATR spike
    """

    if "ATR_breakout" not in cls.df:
        logging.error(f"ATP breakout not enough data for symbol: {cls.symbol}")
        return

    green_candle = cls.df["close"] > cls.df["open"]
    volume_confirmation = cls.df["volume"] > cls.df["volume"].rolling(20).mean()

    adp_diff = cls.market_breadth_data["adp"][-1] - cls.market_breadth_data["adp"][-2]
    adp_diff_prev = (
        cls.market_breadth_data["adp"][-2] - cls.market_breadth_data["adp"][-3]
    )

    if (
        (
            cls.df["ATR_breakout"].iloc[-1]
            or cls.df["ATR_breakout"].iloc[-2]
            or cls.df["ATR_breakout"].iloc[-3]
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
        close_price = cls.df["close"].iloc[-1]

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {cls.bot_strategy.value}
        - BTC correlation: {round_numbers(cls.btc_correlation)}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
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

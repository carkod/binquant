import logging
import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy
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
    # if cls.df_4h.isnull().values.any() or cls.df_4h.size == 0:
    #     logging.error("4h candles twap momentum have null values")
    #     return

    last_twap = cls.df_1h["twap"].iloc[-1]
    price_decrease = (
        cls.df_1h["close"].iloc[-1]
        - cls.df_1h["close"].iloc[-2] / cls.df_1h["close"].iloc[-1]
    )

    if last_twap > close_price and price_decrease > -0.05:
        algo = "coinrule_twap_momentum_sniper"

        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{cls.symbol}
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

    last_supertrend = cls.df_1h["supertrend"].iloc[-1]
    prev_last_supertrend = cls.df_1h["supertrend"].iloc[-2]
    last_rsi = round_numbers(cls.df_1h["rsi"].iloc[-1])
    # prev_last_rsi = round_numbers(cls.df_1h["rsi"].iloc[-2])
    prev_close_price = cls.df_1h["close"].iloc[-2]

    if (last_supertrend < close_price and prev_last_supertrend < prev_close_price) and (
        last_rsi < 30
    ):
        algo = "coinrule_supertrend_swing_reversal"
        bb_high, bb_mid, bb_low = cls.bb_spreads()
        bot_strategy = Strategy.long

        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {bot_strategy.value}
        - RSI smaller than 30: {last_rsi}
        - Supertrend larger than current price: {round_numbers(last_supertrend)}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=False,
            spread=None,
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

    if rsi < 35 and close_price > ma_25 and volatility > 0.01:
        algo = "coinrule_buy_low_sell_high"
        volatility = round_numbers(volatility, 6)

        # market is bearish, most prices decreasing, (LOSERS)
        # but looks like it's picking up and going bullish (reversal)
        # candlesticks of this specific crypto are seeing a huge drop (candlstick drop algo)
        if (
            cls.market_domination_reversal
            and cls.current_market_dominance == MarketDominance.LOSERS
        ):
            bot_strategy = Strategy.long
        else:
            return

        msg = f"""
        - [{os.getenv('ENV')}] <strong>{algo} #algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - Strategy: {bot_strategy.value}
        - Reversal? {"No reversal" if not cls.market_domination_reversal else "Positive" if cls.market_domination_reversal else "Negative"}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
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

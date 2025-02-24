import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


def top_gainers_drop(
    cls: "TechnicalIndicators",
    close_price,
    open_price,
    volatility,
):
    """
    From the list of USDT top gainers
    pick the first 4, expect them to drop at some point
    so create margin_short bot

    """
    if float(close_price) < float(open_price) and cls.symbol in cls.top_coins_gainers:
        algo = "top_gainers_drop"
        bb_high, bb_mid, bb_low = cls.bb_spreads()

        if (
            cls.market_domination_reversal
            and cls.current_market_dominance == MarketDominance.GAINERS
            or cls.current_market_dominance == MarketDominance.NEUTRAL
        ):
            # market is bullish, most prices increasing,
            # but looks like it's dropping and going bearish (reversal)
            # candlesticks of this specific crypto are seeing a huge jump (candlstick jump algo)
            bot_strategy = Strategy.margin_short
        else:
            return

        msg = f"""
        - [{os.getenv('ENV')}] Top gainers's drop <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
        - Market domination trend: {cls.current_market_dominance}
        - Strategy: {bot_strategy}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            spread=volatility,
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

        cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(cls.base_producer.on_send_success).add_errback(
            cls.base_producer.on_send_error
        )

    return

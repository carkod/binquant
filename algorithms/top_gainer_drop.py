import os

from models.signals import SignalsConsumer
from shared.enums import KafkaTopics


def top_gainers_drop(
    cls,
    close_price,
    open_price,
    ma_7,
    ma_25,
    ma_100,
    ma_7_prev,
    ma_25_prev,
    ma_100_prev,
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

        msg = f"""
        - [{os.getenv('ENV')}] Top gainers's drop <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
        - Market domination trend: {cls.market_domination_trend}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            spread=volatility,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            trend=cls.market_domination_trend,
            bb_spreads=None,
        )

        cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(cls.base_producer.on_send_success).add_errback(
            cls.base_producer.on_send_error
        )

    return

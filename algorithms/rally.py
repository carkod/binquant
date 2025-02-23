import os
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


def rally_or_pullback(
    cls: "TechnicalIndicators",
    close_price,
    ma_25,
    ma_100,
    ma_25_prev,
    ma_100_prev,
    volatility,
):
    """
    Rally algorithm

    Key difference with other algorithms is the trend is determined not by market
    but day or minute percentage change
    https://www.binance.com/en/support/faq/understanding-top-movers-statuses-on-binance-spot-trading-18c97e8ab67a4e1b824edd590cae9f16
    """
    data = cls.ticker_24(symbol=cls.symbol)

    # Rally
    day_diff = (float(data["lowPrice"]) - float(data["openPrice"])) / float(
        data["openPrice"]
    )
    minute_diff = (close_price - float(data["lowPrice"])) / float(data["lowPrice"])

    # Pullback
    day_diff_pb = (float(data["highPrice"]) - float(data["openPrice"])) / float(
        data["openPrice"]
    )
    minute_diff_pb = (close_price - float(data["highPrice"])) / float(data["highPrice"])

    algo_type = None

    if day_diff <= 0.08 and minute_diff >= 0.05:
        algo_type = "Rally"

    if day_diff_pb >= 0.08 and minute_diff_pb <= 0.05:
        algo_type = "Pullback"

    if not algo_type:
        return

    if (
        close_price < ma_25
        and close_price < ma_25
        and close_price < ma_25_prev
        and close_price < ma_100
        and close_price < ma_100_prev
    ):
        bb_high, bb_mid, bb_low = cls.bb_spreads()

        msg = f"""
            - [{os.getenv('ENV')}] <strong>{algo_type} #algorithm</strong> #{cls.symbol}
            - Current price: {close_price}
            - Log volatility (log SD): {volatility}
            - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
            - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
            - TimesGPT forecast: {cls.forecast}
            - https://www.binance.com/en/trade/{cls.symbol}
            - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            spread=None,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo_type,
            bot_strategy=cls.bot_strategy,
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

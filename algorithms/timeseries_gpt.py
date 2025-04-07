import os
from typing import TYPE_CHECKING

from models.signals import SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


def time_gpt_market_domination(
    cls: "TechnicalIndicators", close_price, gainers_count, losers_count, msf
):
    """
    Wrapper function for the TimeseriesGPT class
    """
    forecasted_gainers = msf["gainers_count"].values[0]
    total_count = gainers_count[-1:] + losers_count[-1:]
    forecasted_losers = total_count - forecasted_gainers

    if forecasted_gainers > forecasted_losers:
        # Update current market dominance
        cls.current_market_dominance = MarketDominance.GAINERS

        if (
            gainers_count[-1] > losers_count[-1]
            and gainers_count[-2] > losers_count[-2]
        ):
            cls.market_domination_reversal = True
            cls.bot_strategy = Strategy.long

        algo = "time_gpt_market_domination"

        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Strategy: {cls.bot_strategy.value}
        - <a href='https://www.binance.com/en/trade/{cls.symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=cls.bot_strategy,
            autotrade=False,
            bb_spreads=None,
        )

        cls.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(cls.base_producer.on_send_success).add_errback(
            cls.base_producer.on_send_error
        )

import os

from kafka import KafkaProducer

from producers.base import BaseProducer
from shared.enums import Strategy

"""
Base class to hold common methods
for all algorithms
"""


class AlgoBase:
    def __init__(self, model, **kwargs):
        self.base_producer = BaseProducer()
        self.base_producer.start_producer()
        self.producer: KafkaProducer = self.base_producer.producer

    def message_construction(
        self,
        symbol,
        algo,
        close_price: float = 0,
        volatility: float = 0,
        market_domination_reversal: bool | None = None,
        btc_correlation: float | None = None,
        bot_strategy: Strategy | None = None,
        bb_high: float = 0,
        bb_low: float = 0,
        forecast: float = 0,
    ):
        """
        Provide a common interface and consistent
        for telegram message content

        Some of these args must be by default None
        in order to not send data that we don't need
        in the message string
        """

        if close_price > 0:
            close_price_line = f"Current price: {close_price}\n"

        if volatility > 0:
            volatility_line = f"Log volatility (log SD):  {volatility:.2f}"

        if market_domination_reversal is not None:
            market_domination_reversal_line = (
                f'Reversal? {"Yes" if market_domination_reversal else "No"}'
            )

        if bot_strategy is not None:
            bot_strategy_line = f"Strategy: {bot_strategy.value}"

        if bb_high > 0 and bb_low > 0:
            bb_line = f"Bollinguer bands spread: {(bb_high - bb_low) / bb_high }"

        if btc_correlation is not None:
            btc_correlation_line = f"BTC correlation: {btc_correlation}"

        if forecast > 0:
            forecast_line = f"TimesGPT forecast: {forecast}"

        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{symbol}
        - {close_price_line}
        - {volatility_line}
        - {market_domination_reversal_line}
        - {bot_strategy_line}
        - {bb_line}
        - {btc_correlation_line}
        - {forecast_line}
        - <a href='https://www.binance.com/en/trade/{symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
        """

        return msg

    # def model_and_produce(self):
    #     """
    #     Provide a common and consistent interface
    #     for signal production
    #     """

    #     self.message_construction(spread, close_price, volatility, market_domination_reversal, btc_correlation, bot_strategy, bb_high, bb_low, forecast)

    #     value = SignalsConsumer(
    #         spread=spread,
    #         current_price=close_price,
    #         msg=msg,
    #         symbol=cls.symbol,
    #         algo=algo,
    #         bot_strategy=bot_strategy,
    #     )

    #     self.producer.send(
    #         KafkaTopics.signals.value, value=value.model_dump_json()
    #     )

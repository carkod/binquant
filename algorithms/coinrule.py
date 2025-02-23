import os

from models.signals import SignalsConsumer
from shared.enums import KafkaTopics
from shared.utils import round_numbers


def fast_and_slow_macd(
    self, close_price, symbol, macd, macd_signal, ma_7, ma_25, ma_100, volatility
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Fast-EMA-above-Slow-EMA-with-MACD-6f8653

    """
    algo = "coinrule_fast_and_slow_macd"
    volatility = round_numbers(volatility, 6)
    spread = volatility
    bb_high, bb_mid, bb_low = self.bb_spreads()

    # If volatility is too low, dynamic trailling will close too early with bb_spreads
    if macd > macd_signal and ma_7 > ma_25 and bb_high < 1 and bb_high > 0.001:
        msg = f"""
        - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{symbol} 
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Reversal? {"Yes" if self.market_domination_reversal else "No"}
        - Strategy: {self.market_domination_trend}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - TimesGPT forecast: {self.forecast}
        - <a href='https://www.binance.com/en/trade/{symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            spread=spread,
            current_price=close_price,
            msg=msg,
            symbol=symbol,
            algo=algo,
            trend=self.market_domination_trend,
            bb_spreads={
                "bb_high": bb_high,
                "bb_mid": bb_mid,
                "bb_low": bb_low,
            },
        )

        self.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(self.base_producer.on_send_success).add_errback(
            self.base_producer.on_send_error
        )

    pass


def buy_low_sell_high(self, close_price, symbol, rsi, ma_25, ma_7, ma_100, volatility):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Multi-Time-Frame-Buy-Low-Sell-High-Short-term-8f02df
    """
    volatility = round_numbers(volatility, 6)
    bb_high, bb_mid, bb_low = self.bb_spreads()

    if rsi < 35 and close_price > ma_25 and volatility > 0.01:
        algo = "coinrule_buy_low_sell_high"
        volatility = round_numbers(volatility, 6)

        if not self.market_domination_trend:
            return

        # Second stage filtering when volatility is high
        # when volatility is high we assume that
        # difference between MA_7 and MA_25 is wide
        # if this is not the case it may fail to signal correctly
        bb_high, bb_mid, bb_low = self.bb_spreads()

        msg = f"""
    - [{os.getenv('ENV')}] <strong>{algo} #algorithm</strong> #{symbol}
    - Current price: {close_price}
    - Log volatility (log SD): {volatility}
    - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
    - Strategy: {self.market_domination_trend}
    - Reversal? {"No reversal" if not self.market_domination_reversal else "Positive" if self.market_domination_reversal else "Negative"}
    - TimesGPT forecast: {self.forecast}
    - https://www.binance.com/en/trade/{symbol}
    - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
    """

        value = SignalsConsumer(
            spread=0,
            current_price=close_price,
            msg=msg,
            symbol=symbol,
            algo=algo,
            trend=self.market_domination_trend,
            bb_spreads={
                "bb_high": bb_high,
                "bb_mid": bb_mid,
                "bb_low": bb_low,
            },
        )

        self.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(self.base_producer.on_send_success).add_errback(
            self.base_producer.on_send_error
        )

    pass

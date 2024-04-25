import json
import os

from models.signals import SignalsConsumer
from shared.enums import KafkaTopics


def fast_and_slow_macd(
    self,
    close_price,
    symbol,
    macd,
    macd_signal,
    ma_7,
    ma_25,
    ma_100,
    volatility
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Fast-EMA-above-Slow-EMA-with-MACD-6f8653

    """
    algo = "coinrule_fast_and_slow_macd"
    spread = None
    strategy = self.define_strategy()

    if macd > macd_signal and ma_7 > ma_25:

        bb_high, bb_mid, bb_low = self.bb_spreads(strategy)

    # Second stage filtering when volatility is high
    # when volatility is high we assume that
    # difference between MA_7 and MA_25 is wide
    # if this is not the case it may fail to signal correctly
    # if self.volatility > 0.8:

    # Calculate spread using bolliguer band MAs

        msg = (f"""
        - [{os.getenv('ENV')}] <strong>{algo} #algorithm</strong> #{symbol} 
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}%
        - <a href='https://www.binance.com/en/trade/{symbol}'>Binance</a>
        - <a href='http://terminal.binbot.in/admin/bots/new/{symbol}'>Dashboard trade</a>
        """)

        value = SignalsConsumer(
            spread=spread,
            current_price=close_price,
            msg=msg,
            symbol=symbol,
            algo=algo,
            bb_spreads={
                "bb_high": bb_high,
                "bb_mid": bb_mid,
                "bb_low": bb_low,
            }
        )

        self.producer.send(KafkaTopics.signals.value, value=value.model_dump_json()).add_callback(self.base_producer.on_send_success).add_errback(self.base_producer.on_send_error)

    pass


def buy_low_sell_high(
    self,
    close_price,
    symbol,
    rsi,
    ma_25,
    ma_7,
    ma_100,
    volatility
):
    """
    Coinrule top performance rule
    https://web.coinrule.com/share-rule/Multi-Time-Frame-Buy-Low-Sell-High-Short-term-8f02df
    """

    if rsi[str(len(rsi) - 1)] < 35 and close_price > ma_25[len(ma_25) - 1]:

        spread = None
        algo = "coinrule_buy_low_sell_high"
        trend = self.define_strategy()

        if not trend:
            return

        bb_high, bb_mid, bb_low = self.bb_spreads(trend)

        # Second stage filtering when volatility is high
        # when volatility is high we assume that
        # difference between MA_7 and MA_25 is wide
        # if this is not the case it may fail to signal correctly
        if volatility > 0.8:

            # Calculate spread using bolliguer band MAs
            spread = self.bollinguer_spreads(ma_100, ma_25, ma_7)
        
        msg = (f"""
- [{os.getenv('ENV')}] <strong>{algo} #algorithm</strong> #{symbol}
- Current price: {close_price}
- Log volatility (log SD): {self.volatility}%
- Bollinguer bands spread: {spread['band_1']}, {spread['band_2']}
- Strategy: {trend}
- Reversal? {"No reversal" if not self.market_domination_reversal else "Positive" if self.market_domination_reversal else "Negative"}
- https://www.binance.com/en/trade/{symbol}
- <a href='http://terminal.binbot.in/admin/bots/new/{symbol}'>Dashboard trade</a>
""")
        
        value = SignalsConsumer(
            spread=spread,
            current_price=close_price,
            msg=msg,
            symbol=symbol,
            algo=algo,
            bb_spreads={
                "bb_high": bb_high,
                "bb_mid": bb_mid,
                "bb_low": bb_low,
            }
        )

        self.producer.send(KafkaTopics.signals.value, value=value.model_dump_json()).add_callback(self.base_producer.on_send_success).add_errback(self.base_producer.on_send_error)

    pass

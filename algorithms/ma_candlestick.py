import os

from models.signals import SignalsConsumer
from shared.enums import KafkaTopics
from shared.utils import round_numbers

# Algorithms based on Bollinguer bands


def ma_candlestick_jump(
    self,
    close_price,
    open_price,
    symbol,
    ma_7,
    ma_25,
    ma_100,
    ma_7_prev,
    ma_25_prev,
    ma_100_prev,
    volatility,
):
    """
    Candlesticks are in an upward trending motion for several periods
    This algorithm checks last close prices > MAs to decide whether to trade

    Intercept: the larger the value, the higher the potential for growth
        e.g. Given predictor y = 0.123x + 2.5, for x = 1, y = 0.123 + 2.5 = 2.623
             Given predictor y = 0.123x + 10, for x = 1, y = 0.123 + 10 = 10.123

    Chaikin_diff: positive values indicate overbought, negative values indicate oversold
    - Buy when oversold, sell when overbought

    SD: standard deviation of 0.006 seems to be a good threshold after monitoring signals,
    whereas it is possible to get around 3% increase to actually make a profit
    """
    volatility = round_numbers(volatility, 6)
    bb_high, bb_mid, bb_low = self.bb_spreads()

    if (
        float(close_price) > float(open_price)
        and bb_high < 1
        and bb_high > 0.01
        and close_price > ma_7
        and open_price > ma_7
        and close_price > ma_25
        and open_price > ma_25
        and ma_7 > ma_7_prev
        and close_price > ma_7_prev
        and open_price > ma_7_prev
        and close_price > ma_100
        and open_price > ma_100
    ):
        algo = "ma_candlestick_jump"
        spread = volatility
        msg = f"""
- [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{symbol}
- Current price: {close_price}
- %threshold based on volatility: {volatility}
- Reversal? {"Yes" if self.market_domination_reversal else "No"}
- Strategy: {self.market_domination_trend}
- Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
- TimesGPT forecast: {self.forecast}
- https://www.binance.com/en/trade/{symbol}
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

    return


def ma_candlestick_drop(
    self,
    close_price,
    open_price,
    symbol,
    ma_7,
    ma_100,
    ma_25,
    ma_7_prev,
    ma_25_prev,
    ma_100_prev,
    volatility,
):
    """
    Opposite algorithm of ma_candletick_jump
    This algorithm detects Candlesticks that are in a downard trending motion for several periods

    Suitable for margin short trading (borrow - margin sell - buy back - repay)
    """
    volatility = round_numbers(volatility, 6)
    if (
        float(close_price) < float(open_price)
        and volatility > 0.009
        and close_price < ma_7
        and open_price < ma_7
        and close_price < ma_25
        and open_price < ma_25
        and ma_7 < ma_25_prev
        and close_price < ma_25_prev
        and open_price < ma_25_prev
        and close_price < ma_100
        and open_price < ma_100
        # remove high standard deviation
        # big candles. too many signals with little profitability
        and (abs(float(close_price) - float(open_price)) / float(close_price)) > 0.02
    ):
        algo = "ma_candlestick_drop"
        bb_high, bb_mid, bb_low = self.bb_spreads()

        msg = f"""
- [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{symbol}
- Current price: {close_price}
- Log volatility (log SD): {volatility}
- Reversal? {self.market_domination_reversal}
- Strategy: {self.market_domination_trend}
- Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
- TimesGPT forecast: {self.forecast}
- https://www.binance.com/en/trade/{symbol}
- <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
"""

        value = SignalsConsumer(
            spread=None,
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

    return

import os

from models.signals import SignalsConsumer
from shared.enums import KafkaTopics


def price_rise_15(
    self,
    close_price,
    symbol,
    prev_price,
    p_value,
    btc_correlation,
):
    """
    Price increase/decrease algorithm

    https://www.binance.com/en/support/faq/understanding-top-movers-statuses-on-binance-spot-trading-18c97e8ab67a4e1b824edd590cae9f16
    """

    algo = "price_rise_15_rally_pullback"
    price_diff = (float(close_price) - float(prev_price)) / close_price
    bb_high, bb_mid, bb_low = self.bb_spreads()
    trend = self.define_strategy()

    if 0.07 <= price_diff < 0.11:
        first_line = "<strong>Price increase</strong> over 7%"

    elif -0.07 <= price_diff < -0.11:
        first_line = f"<strong>{algo} #algorithm</strong> over 7%"

    else:
        return

    msg = f"""
- [{os.getenv('ENV')}] {first_line} #{symbol}
- Current price: {close_price}
- P-value: {p_value}
- Pearson correlation with BTC: {btc_correlation["close_price"]}
- https://www.binance.com/en/trade/{symbol}
- <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
"""

    value = SignalsConsumer(
        current_price=close_price,
        msg=msg,
        symbol=symbol,
        algo=algo,
        trend=trend,
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

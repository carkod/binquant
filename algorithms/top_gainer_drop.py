import os

from shared.enums import KafkaTopics


def top_gainers_drop(
    self,
    close_price,
    open_price,
    ma_7,
    ma_100,
    ma_25,
    symbol,
    lowest_price,
    slope,
    volatility,
):
    """
    From the list of USDT top gainers
    pick the first 4, expect them to drop at some point
    so create margin_short bot

    """
    if (
        float(close_price) < float(open_price)
        and symbol in self.top_coins_gainers
    ):
        algo = "top_gainers_drop"
        
        trend = self.define_strategy(self)
        if not trend:
            return

        msg = (f"""
- [{os.getenv('ENV')}] Top gainers's drop <strong>#{algo} algorithm</strong> #{symbol}
- Current price: {close_price}
- Log volatility (log SD): {volatility}
- Slope: {slope}
- https://www.binance.com/en/trade/{symbol}
- <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
""")
        value = {
            "msg": msg,
            "symbol": symbol,
            "algo": algo, 
            "spread": volatility,
            "current_price": close_price,
            "trend": trend
        }

        self.producer.send(KafkaTopics.signals.value, value=value.model_dump_json()).add_callback(self.base_producer.on_send_success).add_errback(self.base_producer.on_send_error)

    return

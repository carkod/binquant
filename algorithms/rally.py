import os
from api.streaming.models import SignalsConsumer
from shared.enums import KafkaTopics


def rally_or_pullback(
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
    Rally algorithm

    https://www.binance.com/en/support/faq/understanding-top-movers-statuses-on-binance-spot-trading-18c97e8ab67a4e1b824edd590cae9f16
    """
    data = self.get_24_ticker(symbol)

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
        # trend = "uptrend"

    if day_diff_pb >= 0.08 and minute_diff_pb <= 0.05:
        algo_type = "Pullback"
        # trend = "downtrend"

    if not algo_type:
        return

    trend = self.define_strategy()
    if not trend:
        return

    bb_high, bb_mid, bb_low = self.bb_spreads()

    msg = f"""
        - [{os.getenv('ENV')}] <strong>{algo_type} #algorithm</strong> #{symbol}
        - Current price: {close_price}
        - Log volatility (log SD): {volatility}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high }
        - Reversal? {"Yes" if self.market_domination_reversal else "No"}
        - https://www.binance.com/en/trade/{symbol}
        - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
    """

    if algo_type == "Pullback":
        algo = f"rally_{algo_type}"

        if (
            float(close_price) > float(open_price)
            and volatility > 0.09
            # and close_price < ma_25[len(ma_25) - 1]
            # and close_price < ma_25[len(ma_25) - 2]
            # and close_price < ma_25[len(ma_25) - 3]
            and close_price < ma_100[len(ma_100) - 1]
            and close_price < ma_100[len(ma_100) - 2]
            and close_price < ma_100[len(ma_100) - 3]
        ):
            value = SignalsConsumer(
                spread=None,
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

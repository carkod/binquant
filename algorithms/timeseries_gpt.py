import json
import os
import pandas as pd
from shared.enums import KafkaTopics
from shared.utils import round_numbers
from models.signals import SignalsConsumer
from shared.enums import KafkaTopics
from nixtla import NixtlaClient

nixtla_client = NixtlaClient(
    os.environ.get("NIXTLA_API_KEY")
)

# Algorithms based on Bollinguer bands

def detect_anomalies(
    self,
):
    """
    Test times GPT
    """
    # Remove _id object
    df = self.df.copy()
    df = df.drop(columns=["_id"])
    # volatility = round_numbers(volatility, 6)
    # Detect anomalies
    anomalies_df = nixtla_client.detect_anomalies(
        df,
        time_col="close_time",
        target_col="close",
        freq="min"
    )

    level = [50,80,90] # confidence levels 
    fcst = nixtla_client.forecast(df, h=7, level=level)

    print(anomalies_df)
    print(fcst)

#     if (
#         float(close_price) > float(open_price)
#         and close_price > ma_7
#         and open_price > ma_7
#         and close_price > ma_25
#         and open_price > ma_25
#         and ma_7 > ma_7_prev
#         and close_price > ma_7_prev
#         and open_price > ma_7_prev
#         and close_price > ma_100
#         and open_price > ma_100
#     ):

#         algo = "detect_anomalies"
#         spread = volatility
#         trend = self.define_strategy()
#         msg = (f"""
# - [{os.getenv('ENV')}] Candlestick <strong>#{algo}</strong> #{symbol}
# - Current price: {close_price}
# - %threshold based on volatility: {volatility}
# - Strategy: {trend}
# - https://www.binance.com/en/trade/{symbol}
# - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
# """)

#         value = SignalsConsumer(
#             spread=spread,
#             current_price=close_price,
#             msg=msg,
#             symbol=symbol,
#             algo=algo,
#             trend=trend,
#             bb_spreads={}
#         )

#         self.producer.send(KafkaTopics.signals.value, value=value.model_dump_json()).add_callback(self.base_producer.on_send_success).add_errback(self.base_producer.on_send_error)

    return

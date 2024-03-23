import logging
import os
import asyncio

from datetime import datetime
from logging import info
from time import sleep, time
from typing import Literal

import pandas as pd
import requests
from algorithms.coinrule import buy_low_sell_high, fast_and_slow_macd
from algorithms.ma_candlestick import ma_candlestick_drop, ma_candlestick_jump
from algorithms.price_changes import price_rise_15
from algorithms.rally import rally_or_pullback
from algorithms.top_gainer_drop import top_gainers_drop
from scipy import stats
from shared.apis import BinbotApi
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.telegram_bot import TelegramBot
from shared.utils import handle_binance_errors, round_numbers
from websocket import WebSocketException

from outbound_data.signals_outbound import SignalsOutbound

from kafka import KafkaConsumer
import json
from datetime import datetime
import pandas as pd


from shared.enums import KafkaTopics

async def main():

    print("Consumer started")

    # Create a consumer instance
    klines_consumer = KafkaConsumer(
        KafkaTopics.processed_klines_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )

    # Start consuming
    klines_data = klines_consumer.poll()
    print("Consumer:", klines_data)

if __name__ == "__main__":
    asyncio.run(main())
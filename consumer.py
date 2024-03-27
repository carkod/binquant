import logging
import os
import asyncio

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


from kafka import KafkaConsumer
import json
from datetime import datetime
import pandas as pd

from aiokafka import AIOKafkaConsumer

from shared.enums import KafkaTopics
from consumers.klines_provider import KlinesProvider

async def main():

    print("Consumer started")
    # Create a consumer instance
    klines_consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
    )

    # Start consuming
    await klines_consumer.start()
    try:
        while True:
            klines_provider = KlinesProvider(klines_consumer)
            async for message in klines_consumer:
                await klines_provider.aggregate_klines_by_offset(message)
                # print(message)
    except Exception as error:
        print(error)
    finally:
        await klines_consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())

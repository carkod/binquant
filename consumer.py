import os
import asyncio

from aiokafka import AIOKafkaConsumer
import requests
import json
import pandas as pd

from shared.enums import KafkaTopics
from consumers.klines_provider import KlinesProvider

async def main():

    # Create a consumer instance
    klines_consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
    )
    # Start consuming
    await klines_consumer.start()
    while True:
        try:
        
            klines_provider = KlinesProvider(klines_consumer)
            await klines_provider.technical_analyses()

        except Exception as error:
            print(error)
        finally:
            await klines_consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())

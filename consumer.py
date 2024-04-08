import json
import os
import asyncio
import logging

from aiokafka import AIOKafkaConsumer
import telegram

from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider
from py4j.protocol import Py4JNetworkError

async def main():

    # Start consuming
    consumer = AIOKafkaConsumer(
            KafkaTopics.klines_store_topic.value,
            KafkaTopics.signals.value,
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            # group_id="klines",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m),
        )

    klines_provider = KlinesProvider(consumer)
    telegram_consumer = TelegramConsumer(consumer)

    await consumer.start()
    
    tasks = []
    try:

        async for result in consumer:
            if result.topic == KafkaTopics.klines_store_topic.value:
                tasks.append(asyncio.create_task(klines_provider.aggregate_data(result)))
            
            if result.topic == KafkaTopics.signals.value:
                tasks.append(asyncio.create_task(telegram_consumer.send_telegram(result)))
        
    except Py4JNetworkError:
        asyncio.run(main())

    except Exception as error:
        print(error)
    finally:
        await consumer.stop()

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    while True:
        asyncio.run(main())

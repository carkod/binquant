import json
import os
import asyncio

from aiokafka import AIOKafkaConsumer
from kafka import KafkaConsumer
from consumers.autotrade_consumer import AutotradeConsumer
from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider

def task_1():

    # Start consuming
    consumer = KafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        enable_auto_commit=True,
    )

    klines_provider = KlinesProvider(consumer)
    for result in consumer:
        klines_provider.aggregate_data(result)


def task_2():
    consumer = KafkaConsumer(
        KafkaTopics.signals.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
    )

    telegram_consumer = TelegramConsumer(consumer)
    at_consumer = AutotradeConsumer(consumer)

    for message in consumer:
        print("Received signal for telegram and autotrade! task_2", message.value)
        if message.topic == KafkaTopics.restart_streaming.value:
            at_consumer.load_data_on_start()
        if message.topic == KafkaTopics.klines_store_topic.value:
            # telegram_consumer.send_telegram(result)
            at_consumer.process_autotrade_restrictions(message.value)


async def main():
    await asyncio.gather(asyncio.to_thread(task_1), asyncio.to_thread(task_2))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        asyncio.run(main())
        pass

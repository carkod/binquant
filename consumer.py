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
    for result in consumer:
        print("Received signal for telegram and autotrade! task_2", result.value)
        telegram_consumer.send_telegram(result)
        at_consumer.process_autotrade_restrictions(result)

async def task_3():

    # Start consuming
    consumer = AIOKafkaConsumer(
        KafkaTopics.signals.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
    )

    telegram_consumer = TelegramConsumer(consumer)
    at_consumer = AutotradeConsumer(consumer)

    await consumer.start()
    
    tasks = []
    # result = await consumer.getmany()
    async for result in consumer:
        print("Received signal for telegram and autotrade! task_3", result)
        tasks.append(asyncio.create_task(telegram_consumer.send_telegram(result)))
        tasks.append(asyncio.create_task(at_consumer.process_autotrade_restrictions(result)))

    await consumer.stop()
    return tasks

async def main():
    await asyncio.gather(asyncio.to_thread(task_1), asyncio.to_thread(task_2))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        asyncio.run(main())
        pass
    

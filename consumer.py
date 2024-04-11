import json
import os
import asyncio

from aiokafka import AIOKafkaConsumer
from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider

async def task_1():
    # Start consuming
    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        group_id="klines_group",
        # enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m),
    )

    klines_provider = KlinesProvider(consumer)

    await consumer.start()
    
    tasks = []
    async for result in consumer:
        tasks.append(asyncio.create_task(klines_provider.aggregate_data(result)))

    await consumer.flush()
    return tasks


async def task_2():

    # Start consuming
    consumer = AIOKafkaConsumer(
        KafkaTopics.signals.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        group_id="signals_group",
        # enable_auto_commit=False,
        # auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m),
    )

    telegram_consumer = TelegramConsumer(consumer)

    await consumer.start()
    
    tasks = []
    async for result in consumer:
        tasks.append(asyncio.create_task(telegram_consumer.send_telegram(result)))

    await consumer.flush()
    return tasks


async def main():
    try:
        await asyncio.gather(task_1(), task_2())
    except Exception:
        asyncio.run(main())
        pass

if __name__ == "__main__":
    asyncio.run(main())

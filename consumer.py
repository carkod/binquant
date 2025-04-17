import asyncio
import concurrent.futures
import json
import logging
import os

from aiokafka import AIOKafkaConsumer

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider
from consumers.telegram_consumer import TelegramConsumer
from shared.enums import KafkaTopics


async def data_process_pipe() -> None:
    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="klines-consumer",
    )

    await consumer.start()
    klines_provider = KlinesProvider(consumer)

    async for message in consumer:
        if message.topic == KafkaTopics.klines_store_topic.value:
            klines_provider.aggregate_data(message.value)

    await consumer.commit()


async def data_analytics_pipe() -> None:
    consumer = AIOKafkaConsumer(
        KafkaTopics.signals.value,
        KafkaTopics.restart_streaming.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        max_poll_records=50,
    )
    await consumer.start()
    telegram_consumer = TelegramConsumer()
    at_consumer = AutotradeConsumer(consumer)

    async for message in consumer:
        if message.topic == KafkaTopics.restart_streaming.value:
            raise Exception("Received restart streaming signal, restarting streaming.")

        if message.topic == KafkaTopics.signals.value:
            await telegram_consumer.send_msg(message.value)
            at_consumer.process_autotrade_restrictions(message.value)


async def main() -> None:
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await asyncio.gather(
            loop.run_in_executor(pool, asyncio.run, data_process_pipe()),
            loop.run_in_executor(pool, asyncio.run, data_analytics_pipe()),
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(os.environ["LOG_LEVEL"])
    logger = logging.getLogger("aiokafka")

    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}")
        asyncio.run(main())

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
from aiokafka.structs import TopicPartition


async def data_analytics_pipe() -> None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-analytics-group",
        enable_auto_commit=False,
        session_timeout_ms=120000,
        heartbeat_interval_ms=120000,
        request_timeout_ms=125000,
    )

    try:
        telegram_consumer = TelegramConsumer()
        at_consumer = AutotradeConsumer(consumer)
        await consumer.start()

        # Fetch the total number of partitions for each topic
        topic_partitions = [
            TopicPartition(topic, partition)
            for topic in [KafkaTopics.signals.value, KafkaTopics.restart_streaming.value]
            for partition in (await consumer.partitions_for_topic(topic) or [])
        ]

        consumer.assign(topic_partitions)  # Assign partitions exclusively
        for tp in topic_partitions:
            await consumer.seek_to_end(tp)  # Move offset to the end of each partition

        # Consume messages and print their values
        async for message in consumer:
            if message.topic == KafkaTopics.restart_streaming.value:
                raise Exception("Received restart streaming signal, restarting streaming.")

            if message.topic == KafkaTopics.signals.value:
                await telegram_consumer.send_msg(message.value)
                at_consumer.process_autotrade_restrictions(message.value)

            await consumer.commit()

    except Exception as e:
        logging.error(f"Error in data_analytics_pipe: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.getLogger("aiokafka").setLevel(os.environ["LOG_LEVEL"])

    try:
        asyncio.run(data_analytics_pipe())
    except Exception as e:
        logging.error(f"Error in main: {e}")
        asyncio.run(data_analytics_pipe())

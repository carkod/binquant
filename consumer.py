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
from producers.base import BaseProducer


async def data_process_pipe() -> None:
    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-process-group",
        enable_auto_commit=False,
        session_timeout_ms=300000,  # Increase session timeout to 5 minutes
        heartbeat_interval_ms=300000,  # Keep heartbeat interval at 10 seconds
        request_timeout_ms=305000,  # Increase request timeout to slightly more than session timeout
    )

    base_producer = BaseProducer()
    base_producer.start_producer()
    producer = base_producer.producer

    try:
        # Start dependencies before the consumer to avoid timeouts
        # klines_provider = KlinesProvider(consumer)
        await consumer.start()
        count = 0

        async for message in consumer:
            if message.topic == KafkaTopics.klines_store_topic.value:
                count += 1
                value = f"""{{"symbol":"ADAUSDC","open_time":"1745099760000","close_time":"1745099819999","open_price":"0.62980000","close_price":"0.62980000","high_price":"0.62980000","low_price":"0.62980000","volume":560.0, "count": {count}}}"""
                producer.send(KafkaTopics.signals.value, value=f"{count}")
                # klines_provider.aggregate_data(value)

            await consumer.commit()

    except Exception as e:
        logging.error(f"Error in data_process_pipe: {e}")
    finally:
        await consumer.stop()


async def data_analytics_pipe() -> None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-analytics-group",
        session_timeout_ms=300000,  # Increase session timeout to 5 minutes
        heartbeat_interval_ms=300000,  # Keep heartbeat interval at 10 seconds
        request_timeout_ms=305000,  # Increase request timeout to slightly more than session timeout
        enable_auto_commit=False,
    )

    try:
        await consumer.start()

        # Seek to the end of each partition to consume only the latest messages
        from aiokafka.structs import TopicPartition
        topic_partitions = [
            TopicPartition(topic, partition)
            for topic in [KafkaTopics.signals.value, KafkaTopics.restart_streaming.value]
            for partition in range(15)  # Assuming 15 partitions
        ]

        consumer.assign(topic_partitions)  # Assign partitions exclusively
        for tp in topic_partitions:
            await consumer.seek_to_end(tp)  # Move offset to the end of each partition

        # Consume messages and print their values
        async for message in consumer:
            print(f"Received message: topic={message.topic}, partition={message.partition}, offset={message.offset}, value={message.value}")
            if message.topic == KafkaTopics.restart_streaming.value:
                raise Exception("Received restart streaming signal, restarting streaming.")

            if message.topic == KafkaTopics.signals.value:
                print(f"Received signal: {message.value}")
            await consumer.commit()

    except Exception as e:
        logging.error(f"Error in data_analytics_pipe: {e}")
    finally:
        await consumer.stop()


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

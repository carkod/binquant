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
from shared.rebalance_listener import RebalanceListener


async def data_process_pipe() -> None:
    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-process-group",
        enable_auto_commit=False,
        session_timeout_ms=30000,
        heartbeat_interval_ms=30000,
        request_timeout_ms=30000,
    )

    # Set rebalance listener
    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.klines_store_topic.value], listener=rebalance_listener
    )

    # Start dependencies before the consumer to avoid timeouts
    klines_provider = KlinesProvider(consumer)
    await klines_provider.load_data_on_start()
    await consumer.start()

    try:
        async for message in consumer:
            if message.topic == KafkaTopics.klines_store_topic.value:
                await klines_provider.aggregate_data(message.value)

            await consumer.commit()

    finally:
        await consumer.stop()


async def data_analytics_pipe() -> None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-analytics-group",
        enable_auto_commit=False,
        session_timeout_ms=30000,
        heartbeat_interval_ms=30000,
        request_timeout_ms=30000,
    )

    # Set rebalance listener
    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.signals.value, KafkaTopics.restart_streaming.value],
        listener=rebalance_listener,
    )

    try:
        telegram_consumer = TelegramConsumer()
        at_consumer = AutotradeConsumer(consumer)
        await consumer.start()

        # Consume messages and print their values
        async for message in consumer:
            if message.topic == KafkaTopics.restart_streaming.value:
                raise Exception(
                    "Received restart streaming signal, restarting streaming."
                )

            if message.topic == KafkaTopics.signals.value:
                logging.info("Received signals")
                await telegram_consumer.send_msg(message.value)
                await at_consumer.process_autotrade_restrictions(message.value)

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
    logging.getLogger("aiokafka").setLevel(os.environ["LOG_LEVEL"])
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}")
        asyncio.run(main())

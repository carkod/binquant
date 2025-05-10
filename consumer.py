import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider
from consumers.telegram_consumer import TelegramConsumer
from shared.enums import KafkaTopics
from shared.rebalance_listener import RebalanceListener

logging.basicConfig(
    level=os.environ["LOG_LEVEL"],
    filename=None,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def data_process_pipe() -> None:
    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-process-group",
        enable_auto_commit=False,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10_000,
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

    try:
        await consumer.start()
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
        heartbeat_interval_ms=10_000,
        request_timeout_ms=30000,
    )

    # Set rebalance listener
    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.signals.value, KafkaTopics.restart_streaming.value],
        listener=rebalance_listener,
    )
    # Start dependencies before the consumer to avoid timeouts
    telegram_consumer = TelegramConsumer()
    at_consumer = AutotradeConsumer(consumer)

    try:
        await consumer.start()

        # Consume messages and print their values
        async for message in consumer:
            if message.topic == KafkaTopics.restart_streaming.value:
                at_consumer.load_data_on_start()

            if message.topic == KafkaTopics.signals.value:
                await telegram_consumer.send_signal(message.value)
                await at_consumer.process_autotrade_restrictions(message.value)

            await consumer.commit()

    finally:
        await consumer.stop()


async def main() -> None:
    await asyncio.gather(
        data_process_pipe(),
        data_analytics_pipe(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
        logging.info("Attempting to shut down gracefully after failure.")
        try:
            asyncio.run(asyncio.sleep(1))
        except Exception:
            pass
        raise

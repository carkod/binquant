import asyncio
import json
import logging
import os
from asyncio import Semaphore, create_task, gather
from collections import defaultdict

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

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
        bootstrap_servers=f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
        value_deserializer=lambda m: json.loads(m),
        group_id="data-process-group",
        enable_auto_commit=False,
        session_timeout_ms=60000,  # Increased from 30000
        heartbeat_interval_ms=20_000,  # Increased from 10_000
        max_poll_records=2,  # Reduced from 5 to process fewer messages per poll
        max_poll_interval_ms=1200_000,  # Increased from 600_000 (20 minutes)
        request_timeout_ms=60000,  # Added explicit request timeout
    )

    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.klines_store_topic.value], listener=rebalance_listener
    )

    klines_provider = KlinesProvider(consumer)
    await klines_provider.load_data_on_start()

    sem = Semaphore(5)  # Reduced from 10 to limit concurrent processing
    last_offsets = defaultdict(int)
    pending_tasks = []

    async def handle_message(message):
        tp = (message.topic, message.partition)
        async with sem:
            try:
                await klines_provider.aggregate_data(message.value)
                last_offsets[tp] = message.offset
            except Exception as e:
                logging.error(f"Error processing message: {e}", exc_info=True)
                # Still update offset to avoid reprocessing failed messages
                last_offsets[tp] = message.offset

    try:
        await consumer.start()
        async for message in consumer:
            # Dispatch async task
            task = create_task(handle_message(message))
            pending_tasks.append(task)

            if len(pending_tasks) >= 10:  # Reduced from 20 to commit more frequently
                await gather(*pending_tasks)
                pending_tasks.clear()

                # Commit offsets more frequently to avoid rebalance issues
                if last_offsets:
                    offsets = {
                        TopicPartition(topic, partition): offset + 1
                        for (topic, partition), offset in last_offsets.items()
                    }
                    try:
                        await consumer.commit(offsets=offsets)
                        last_offsets.clear()  # Clear after successful commit
                    except Exception as e:
                        logging.error(f"Commit failed: {e}", exc_info=True)

    finally:
        # Wait for remaining tasks to complete
        if pending_tasks:
            await gather(*pending_tasks)

        # Final commit with error handling
        if last_offsets:
            offsets = {
                TopicPartition(topic, partition): offset + 1
                for (topic, partition), offset in last_offsets.items()
            }
            try:
                await consumer.commit(offsets=offsets)
            except Exception as e:
                logging.error(f"Final commit failed: {e}", exc_info=True)

        await consumer.stop()


async def data_analytics_pipe() -> None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
        value_deserializer=lambda m: json.loads(m),
        group_id="data-analytics-group",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10_000,
        request_timeout_ms=30000,
    )

    # Set rebalance listener
    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.signals.value],
        listener=rebalance_listener,
    )
    # Start dependencies before the consumer to avoid timeouts
    telegram_consumer = TelegramConsumer()
    at_consumer = AutotradeConsumer(consumer)

    try:
        await consumer.start()

        # Consume messages and print their values
        async for message in consumer:
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

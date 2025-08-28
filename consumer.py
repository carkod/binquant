import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from consumers.klines_provider import KlinesProvider
from shared.enums import KafkaTopics
from shared.rebalance_listener import RebalanceListener

logging.basicConfig(
    level=os.environ["LOG_LEVEL"],
    filename=None,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def data_process_pipe() -> None:
    """
    Milliseconds are adjusted to minimize CommitFails
    """

    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        KafkaTopics.restart_streaming.value,
        bootstrap_servers=f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
        value_deserializer=lambda m: json.loads(m),
        group_id="data-process-group",
        enable_auto_commit=False,
        session_timeout_ms=60000,
        heartbeat_interval_ms=20000,
        max_poll_records=2,
        max_poll_interval_ms=1200000,
        request_timeout_ms=60000,
    )

    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.klines_store_topic.value, KafkaTopics.restart_streaming.value],
        listener=rebalance_listener,
    )

    klines_provider = KlinesProvider(consumer)
    await klines_provider.load_data_on_start()

    processed_messages = []

    async def handle_message(message):
        try:
            if message.topic == KafkaTopics.restart_streaming.value:
                logging.info("Received restart_streaming message, reloading data...")
                await klines_provider.load_data_on_start()
                # don't commit message
                return False

            await klines_provider.aggregate_data(message.value)

            processed_messages.append(message)
            logging.debug(
                f"Processed message at offset {message.offset} for {message.topic}:{message.partition}"
            )
            return True
        except Exception as e:
            logging.error(f"Error processing message: {e}", exc_info=True)
            # Do NOT mark failed messages as processed
            # Let them be retried on next consumer restart
            return False

    try:
        await consumer.start()
        async for message in consumer:
            # Process message sequentially - wait for completion before next
            success = await handle_message(message)

            if success:
                # Commit immediately after successful processing
                try:
                    offsets = {
                        TopicPartition(message.topic, message.partition): message.offset
                        + 1
                    }
                    await consumer.commit(offsets=offsets)
                    logging.debug(
                        f"Committed offset: {message.offset + 1} for {message.topic}:{message.partition}"
                    )
                    processed_messages.clear()
                except Exception as e:
                    logging.error(f"Commit failed: {e}", exc_info=True)
                    # Don't clear processed_messages on commit failure
                    # Message will be retried on next consumer restart

    finally:
        # No pending tasks in sequential processing mode
        # Final commit is handled per message, so no cleanup needed
        await consumer.stop()


async def main() -> None:
    await asyncio.gather(data_process_pipe())


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

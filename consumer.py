import asyncio
import json
import logging
import os
import uuid

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from consumers.klines_provider import KlinesProvider
from shared.enums import KafkaTopics
from shared.logging_config import configure_logging
from shared.rebalance_listener import RebalanceListener

configure_logging(force=True)


async def data_process_pipe() -> None:
    """
    Milliseconds are adjusted to minimize CommitFails
    """

    # Generate a unique consumer group id each run so we always start fresh at latest offsets
    # (no resuming previous committed offsets)
    random_group_id = f"data-process-{uuid.uuid4()}"

    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        KafkaTopics.restart_streaming.value,
        bootstrap_servers=f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
        value_deserializer=lambda m: json.loads(m),
        group_id=random_group_id,
        # With a brand new group we can just start at latest; keep manual commit logic for explicit control
        auto_offset_reset="latest",
        enable_auto_commit=False,
        # Session/heartbeat tuned slightly lower for faster failure detection
        session_timeout_ms=45000,
        heartbeat_interval_ms=15000,
        # Process one message at a time to minimize per-message latency (sacrifices throughput)
        max_poll_records=1,
        # Allow long processing but we stay sequential; keep generous interval
        max_poll_interval_ms=1200000,
        request_timeout_ms=60000,
        # Fetch settings for low latency: small batches, short wait
        fetch_min_bytes=1,
        fetch_max_wait_ms=25,
        # Limit bytes per partition to avoid large accumulations before poll returns
        max_partition_fetch_bytes=64 * 1024,
    )

    rebalance_listener = RebalanceListener(consumer)
    consumer.subscribe(
        [KafkaTopics.klines_store_topic.value, KafkaTopics.restart_streaming.value],
        listener=rebalance_listener,
    )

    klines_provider = KlinesProvider(consumer)
    await klines_provider.load_data_on_start()

    processed_messages = []
    latest_committed = {}
    pending_commit = None
    commit_lock = asyncio.Lock()
    COMMIT_DEBOUNCE_MS = 15  # tiny delay to allow potential next message; keep latency low

    async def schedule_commit(message):
        """Schedule a near-immediate asynchronous commit with tiny debounce.
        Multiple rapid messages within COMMIT_DEBOUNCE_MS window will coalesce into one commit.
        """
        nonlocal pending_commit
        tp = TopicPartition(message.topic, message.partition)
        # Store next offset (message.offset + 1) so we commit after this message
        latest_committed[tp] = message.offset + 1

        async def do_commit():
            await asyncio.sleep(COMMIT_DEBOUNCE_MS / 1000)
            async with commit_lock:
                offsets = {tp: off for tp, off in latest_committed.items()}
                try:
                    await consumer.commit(offsets=offsets)
                    logging.debug(
                        "Committed offsets: %s", {f"{p.topic}:{p.partition}": o for p, o in offsets.items()}
                    )
                except Exception as e:
                    logging.error(f"Commit failed: {e}", exc_info=True)
                finally:
                    # Clear pending task reference AFTER commit
                    nonlocal pending_commit
                    pending_commit = None

        # If a commit task is already pending, we let it run; offsets map will be updated before execution
        if pending_commit is None:
            pending_commit = asyncio.create_task(do_commit())

    async def handle_message(message):
        try:
            if message.topic == KafkaTopics.restart_streaming.value:
                logging.info("Received restart_streaming message, reloading data...")
                await klines_provider.load_data_on_start()
                return False

            await klines_provider.aggregate_data(message.value)

            processed_messages.append(message)
            # Debug logging kept minimal; consider lowering to trace if too chatty
            logging.debug(
                "Processed offset %s for %s:%s", message.offset, message.topic, message.partition
            )
            return True
        except Exception as e:
            logging.error(f"Error processing message: {e}", exc_info=True)
            # Do NOT mark failed messages as processed
            # Let them be retried on next consumer restart
            return False

    try:
        await consumer.start()
        logging.info(
            "Started consumer with fresh group id %s (auto_offset_reset=latest)",
            random_group_id,
        )
        async for message in consumer:
            # Process message sequentially - wait for completion before next
            success = await handle_message(message)

            if success:
                # Schedule asynchronous commit with tiny debounce to reduce end-to-end latency
                await schedule_commit(message)
                processed_messages.clear()

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

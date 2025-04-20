import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener
from consumers.klines_provider import KlinesProvider
from shared.enums import KafkaTopics
from aiokafka.structs import TopicPartition


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: AIOKafkaConsumer):
        self.consumer = consumer

    """
    Reset offsets every time there is rebalancing
    because consumption is interrupted
    """

    async def reset_offsets(self):
        # Reset offsets to the end of the partitions
        partitions = self.consumer.partitions_for_topic(
            KafkaTopics.klines_store_topic.value
        )
        if partitions:
            for partition in partitions:
                tp = TopicPartition(KafkaTopics.klines_store_topic.value, partition)
                await self.consumer.seek_to_end(tp)

    async def on_partitions_revoked(self, revoked):
        if revoked:
            raise Exception("Partitions revoked, restarting data_process_pipe")

    async def on_partitions_assigned(self, assigned):
        logging.info(f"Partitions assigned: {assigned}")
        # Seek to the end of the assigned partitions to start from the latest offset
        await self.reset_offsets()


async def data_process_pipe() -> None:
    consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        group_id="data-process-group",
        enable_auto_commit=False,
        session_timeout_ms=120000,
        # heartbeat should be sent less than timouts to check connection
        heartbeat_interval_ms=30000,
        request_timeout_ms=125000,
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
                logging.debug(f"Received message: {message.value}")
                await klines_provider.aggregate_data(message.value)

            await consumer.commit()

    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.getLogger("aiokafka").setLevel(os.environ["LOG_LEVEL"])
    asyncio.run(data_process_pipe())

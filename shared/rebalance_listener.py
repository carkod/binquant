from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition

from shared.enums import KafkaTopics


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
            await self.reset_offsets()
        pass

    async def on_partitions_assigned(self, assigned):
        if assigned:
            await self.reset_offsets()

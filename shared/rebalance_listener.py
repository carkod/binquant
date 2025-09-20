import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: AIOKafkaConsumer):
        self.consumer = consumer

    """
    Reset offsets every time there is rebalancing
    because consumption is interrupted
    """

    async def reset_offsets(self):
        """Seek all currently assigned partitions to their end and commit.

        This ensures we always resume from 'now', ignoring any backlog after a rebalance.
        """
        assignment = list(self.consumer.assignment())
        if not assignment:
            return
        try:
            end_offsets = await self.consumer.end_offsets(assignment)
            for tp, end_offset in end_offsets.items():
                self.consumer.seek(tp, end_offset)
            # end_offsets already a dict[TopicPartition, int]
            await self.consumer.commit(end_offsets)
        except Exception as e:
            logging.error(f"RebalanceListener fast-forward failed: {e}")

    async def on_partitions_revoked(self, revoked):
        if revoked:
            await self.reset_offsets()
        pass

    async def on_partitions_assigned(self, assigned):
        if assigned:
            await self.reset_offsets()

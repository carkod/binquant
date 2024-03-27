import json
import os
import math
from datetime import datetime
from kafka import KafkaConsumer
from numpy import diff
from shared.enums import KafkaTopics
from aiokafka import AIOKafkaConsumer, TopicPartition
from models.klines import KlineModel, KlineProducerPayloadModel

class KlinesProvider:
    """
    Pools, processes, agregates and provides klines data
    """
    def __init__(self, consumer):
        self.consumer = consumer
        self.topic_partition = None
         # Number of klines to aggregate, 100+ for MA
        self.klines_horizon = 3
        self.interval = "15m"
        self.partition_index = 0

    def get_model(self, data):
        """
        Validates data with Pydantic model
        """
        raw_data = json.loads(data)
        return KlineProducerPayloadModel.parse_obj(raw_data)

    def update_klines(self):

        pass

    async def aggregate_klines_by_offset(self, message):
        payload = self.get_model(message.value)

        self.topic_partition = TopicPartition(message.topic, payload.partition)
        seek_offset = message.offset - self.klines_horizon
        if seek_offset > 0:
            self.consumer.seek(self.topic_partition, seek_offset)
            data = await self.consumer.getmany()
            for tp, messages in data.items():
                for msg in messages:
                    candle = payload.kline
                    print("Consumed asset: ", candle.symbol)
                    ot = datetime.fromtimestamp(round(candle.open_time/1000))
                    ct = datetime.fromtimestamp(round(candle.close_time/1000))

                    time_diff = ct - ot
                    print("Time difference: ", time_diff)
                    print("Consumed asset: ", candle.symbol)
        else:
            data = json.loads(message.value)
            # print(f'Not enough data to aggregate {data["symbol"]}')
            pass


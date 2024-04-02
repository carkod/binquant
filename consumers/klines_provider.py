import json
import os
import logging
from aiokafka import AIOKafkaConsumer, TopicPartition
from producers.technical_indicators import TechnicalIndicators
from database import KafkaDB
from shared.enums import KafkaTopics
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark import SparkContext

spark = SparkSession.builder.appName("Klines Statistics analyses")\
    .config("spark.driver.memory", "2g").getOrCreate()

# allow series and/or dataframes to be attached to different dataframes
ps.set_option('compute.ops_on_diff_frames', True)

class KlinesProvider(KafkaDB):
    """
    Pools, processes, agregates and provides klines data
    """
    def __init__(self, consumer: AIOKafkaConsumer):
        super().__init__()
        self.consumer = consumer
        self.topic_partition_ids = []
        self.topic_partition = None
         # Number of klines to aggregate, 100+ for MAosed
        self.klines_horizon = 3
        self.current_partition = 0
        self.candles = []

    def set_partitions(self, partition):
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_deserializer=lambda m: json.loads(m),
        )
        self.consumer.assign([TopicPartition(KafkaTopics.klines_store_topic.value, partition)])

    async def aggregate_data(self, results):

        if results.value:
            payload = json.loads(results.value)
            symbol = payload["symbol"]
            print(f'Consumed: {symbol} @ {payload["close_time"]}')
            candles = self.raw_klines(symbol)
            
            if len(candles) == 0:
                logging.info(f'{symbol} No data to do analytics')
                return

            # self.check_kline_gaps(candles)
            # Pre-process

            # For easier migration, transform into pandas
            # in the future, conversion for RDD may be needed
            # to support Spark scalability
            # spark_df = spark.createDataFrame(candles)

            # ps.options.display.max_rows = 10
            df = ps.DataFrame(candles)
            TechnicalIndicators(df, symbol).publish()

        pass



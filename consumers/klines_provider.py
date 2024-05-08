import json
import logging
import pandas as pd

from kafka import KafkaConsumer
from models.klines import KlineProduceModel
from producers.technical_indicators import TechnicalIndicators
from database import KafkaDB

# spark = SparkSession.builder.appName("Klines Statistics analyses")\
#     .config("compute.ops_on_diff_frames", "true").getOrCreate()

# allow series and/or dataframes to be attached to different dataframes
# ps.set_option('compute.ops_on_diff_frames', True)

class KlinesProvider(KafkaDB):
    """
    Pools, processes, agregates and provides klines data
    """
    def __init__(self, consumer: KafkaConsumer):
        super().__init__()
        # If we don't instantiate separately, almost no messages are received
        self.consumer = consumer

    def get_all_streaming_pairs_count(self):
          
        pass

    def aggregate_data(self, data):

        if data:
            payload = json.loads(data)
            klines = KlineProduceModel.model_validate(payload)
            symbol = klines.symbol
            candles: list[dict] = self.raw_klines(symbol)

            if len(candles) == 0:
                logging.info(f'{symbol} No data to do analytics')
                return

            # self.check_kline_gaps(candles)
            # Pre-process
            raw_df = pd.DataFrame(candles)
            # reverse the order to get the oldest data first, to dropnas and use latest date for technical indicators
            df = raw_df[::-1].reset_index(drop=True)
            TechnicalIndicators(df, symbol).publish()

        pass



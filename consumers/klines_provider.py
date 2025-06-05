import json
import logging
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer

from database import KafkaDB
from models.klines import KlineProduceModel
from producers.base import AsyncProducer
from producers.technical_indicators import TechnicalIndicators
from shared.apis.binbot_api import BinanceApi, BinbotApi
from shared.enums import BinanceKlineIntervals

# spark = SparkSession.builder.appName("Klines Statistics analyses")\
#     .config("compute.ops_on_diff_frames", "true").getOrCreate()

# allow series and/or dataframes to be attached to different dataframes
# ps.set_option('compute.ops_on_diff_frames', True)


class KlinesProvider(KafkaDB):
    """
    Pools, processes, agregates and provides klines data
    """

    def __init__(self, consumer: KafkaConsumer) -> None:
        super().__init__()
        # If we don't instantiate separately, almost no messages are received
        self.binbot_api = BinbotApi()
        self.binance_api = BinanceApi()
        self.consumer = consumer
        # 15 minutes default candles
        self.default_aggregation = {
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "close_time": "last",
            "open_time": "first",
        }
        self.df = pd.DataFrame()
        self.df_4h = pd.DataFrame()
        self.df_1h = pd.DataFrame()

    async def load_data_on_start(self):
        self.active_pairs = self.binbot_api.get_active_pairs()
        base_producer = AsyncProducer().get_producer()
        self.producer = base_producer
        await self.producer.start()
        self.market_domination_data = (
            await self.binbot_api.get_market_domination_series()
        )
        self.top_gainers_day = await self.binbot_api.get_top_gainers()
        self.market_breadth_data = await self.binbot_api.get_market_breadth()

    async def aggregate_data(self, results):
        # Reload time-constrained data
        if datetime.now().minute == 0:
            self.market_domination_data = (
                await self.binbot_api.get_market_domination_series()
            )
            self.top_gainers_day = await self.binbot_api.get_top_gainers()

        if results:
            payload = json.loads(results)
            klines = KlineProduceModel.model_validate(payload)
            symbol = klines.symbol
            candles: list[KlineProduceModel] = self.raw_klines(
                symbol, interval=BinanceKlineIntervals.fifteen_minutes
            )

            if len(candles) == 0:
                logging.warning(f"{symbol} No data to do analytics")
                return

            # Pre-process
            self.df = pd.DataFrame(candles)
            self.df.resample("15Min", on="close_time").agg(self.default_aggregation)
            # Resample to 4 hour candles for TWAP
            self.df_4h = self.df.resample("4h", on="close_time").agg(
                self.default_aggregation
            )
            # Resample to 1 hour candles for Supertrend
            self.df_1h = self.df.resample("1h", on="close_time").agg(
                self.default_aggregation
            )
            # reverse the order to get the oldest data first, to dropnas and use latest date for technical indicators
            self.df = self.df[::-1].reset_index(drop=True)
            technical_indicators = TechnicalIndicators(
                producer=self.producer,
                binbot_api=self.binbot_api,
                df=self.df,
                symbol=symbol,
                df_4h=self.df_4h,
                df_1h=self.df_1h,
                market_domination_data=self.market_domination_data,
                top_gainers_day=self.top_gainers_day,
                market_breadth_data=self.market_breadth_data,
            )
            await technical_indicators.publish()  # Await the async publish method

        pass

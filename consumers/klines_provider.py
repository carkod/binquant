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
        self.top_gainers_day = await self.binbot_api.get_top_gainers()
        self.top_losers_day = await self.binbot_api.get_top_losers()
        self.market_breadth_data = await self.binbot_api.get_market_breadth()

    async def aggregate_data(self, results):
        current_time = datetime.now()

        # Reload time-constrained data every hour
        if current_time.minute == 0:
            self.top_gainers_day = await self.binbot_api.get_top_gainers()
            self.top_losers_day = await self.binbot_api.get_top_losers()
            self.market_breadth_data = await self.binbot_api.get_market_breadth()

        if results:
            payload = json.loads(results)
            klines = KlineProduceModel.model_validate(payload)
            symbol = klines.symbol

            # Refresh klines data every 15 minutes (when minute is 0, 15, 30, or 45)
            if current_time.minute % 15 == 0:
                try:
                    await self.binbot_api.refresh_klines(symbol)
                    logging.info(f"Refreshed klines data for {symbol}")
                except Exception as e:
                    logging.error(f"Failed to refresh klines for {symbol}: {e}")

            candles = self.get_ui_klines(
                symbol, interval=BinanceKlineIntervals.fifteen_minutes.value
            )

            if len(candles) == 0:
                logging.warning(f"{symbol} No data to do analytics")
                return

            # Pre-process
            self.df = pd.DataFrame(candles)
            self.df.columns = [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "unused_field",
            ]

            # Drop unused columns - keep only OHLCV data needed for technical analysis
            self.df = self.df[
                ["open_time", "open", "high", "low", "close", "volume", "close_time"]
            ]

            # Convert price and volume columns to float
            price_volume_columns = ["open", "high", "low", "close", "volume"]
            self.df[price_volume_columns] = self.df[price_volume_columns].astype(float)

            # Ensure close_time is datetime and set as index for proper resampling
            self.df["timestamp"] = pd.to_datetime(self.df["close_time"])
            self.df.set_index("timestamp", inplace=True)

            # Create aggregation dictionary without close_time and open_time since they're now index-based
            resample_aggregation = {
                "open": "first",
                "close": "last",
                "high": "max",
                "low": "min",
                "volume": "sum",  # Add volume if it exists in your data
                "close_time": "first",
                "open_time": "first",
            }

            # Resample to 4 hour candles for TWAP (align to calendar hours like MongoDB)
            self.df_4h = self.df.resample("4h").agg(resample_aggregation)
            # Add open_time and close_time back as columns for 4h data
            self.df_4h["open_time"] = self.df_4h.index
            self.df_4h["close_time"] = self.df_4h.index

            # Resample to 1 hour candles for Supertrend (align to calendar hours like MongoDB)
            self.df_1h = self.df.resample("1h").agg(resample_aggregation)
            # Add open_time and close_time back as columns for 1h data
            self.df_1h["open_time"] = self.df_1h.index
            self.df_1h["close_time"] = self.df_1h.index

            technical_indicators = TechnicalIndicators(
                producer=self.producer,
                binbot_api=self.binbot_api,
                df=self.df,
                symbol=symbol,
                df_4h=self.df_4h,
                df_1h=self.df_1h,
                top_gainers_day=self.top_gainers_day,
                market_breadth_data=self.market_breadth_data,
                top_losers_day=self.top_losers_day,
            )
            await technical_indicators.publish()  # Await the async publish method

        pass

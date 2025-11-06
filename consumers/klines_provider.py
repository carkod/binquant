import json
import logging
from datetime import datetime

from kafka import KafkaConsumer

from consumers.autotrade_consumer import AutotradeConsumer
from models.klines import KlineProduceModel
from producers.analytics import CryptoAnalytics
from shared.apis.binbot_api import BinanceApi, BinbotApi
from shared.enums import BinanceKlineIntervals
from shared.streaming.async_producer import AsyncProducer


class KlinesProvider:
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
        self.all_symbols = self.binbot_api.get_symbols()
        self.producer = AsyncProducer()

    def get_filtered_active_symbols(self) -> list[str]:
        """
        all symbols that are active, not blacklisted
        minus active bots
        minus all symbols that match base asset of these active bots
        i.e. BTC in BTCUSDC
        """
        active_pairs = self.binbot_api.get_active_pairs()
        active_symbols = set(
            {
                s["id"]
                for s in self.all_symbols
                if s["active"] and s["id"] not in active_pairs
            }
        )
        quote_assets = set(
            {
                symbol["base_asset"]
                for symbol in self.all_symbols
                if symbol["id"] in active_pairs
            }
        )
        # Remove all symbols that match quote assets of active bots
        filtered_active_symbols = [
            symbol
            for symbol in active_symbols
            if not any(symbol.startswith(asset) for asset in quote_assets)
        ]
        return filtered_active_symbols

    async def load_data_on_start(self):
        self.producer = await self.producer.start()
        # Klines API dependencies
        self.active_pairs = self.binbot_api.get_active_pairs()
        self.top_gainers_day = await self.binbot_api.get_top_gainers()
        self.top_losers_day = await self.binbot_api.get_top_losers()
        self.market_breadth_data = await self.binbot_api.get_market_breadth()
        active_symbols = self.get_filtered_active_symbols()

        # Autotrade Consumer API dependencies
        self.ac_api = AutotradeConsumer(
            autotrade_settings=self.binbot_api.get_autotrade_settings(),
            active_test_bots=self.binbot_api.get_active_pairs(
                collection_name="paper_trading"
            ),
            all_symbols=self.binbot_api.get_symbols(),
            # Active bot symbols substracting exchange active symbols (not blacklisted)
            active_symbols=active_symbols,
            test_autotrade_settings=self.binbot_api.get_test_autotrade_settings(),
        )

    async def aggregate_data(self, payload):
        current_time = datetime.now()

        # Reload time-constrained data every hour
        if current_time.minute == 0:
            self.top_gainers_day = await self.binbot_api.get_top_gainers()
            self.top_losers_day = await self.binbot_api.get_top_losers()
            self.market_breadth_data = await self.binbot_api.get_market_breadth()

        if payload:
            data = json.loads(payload)
            klines = KlineProduceModel.model_validate(data)
            symbol = klines.symbol

            candles = self.binance_api.get_ui_klines(
                symbol, interval=BinanceKlineIntervals.fifteen_minutes.value
            )

            if len(candles) == 0:
                logging.warning(f"{symbol} No data to do analytics")
                return

            crypto_analytics = CryptoAnalytics(
                producer=self.producer,
                binbot_api=self.binbot_api,
                symbol=symbol,
                top_gainers_day=self.top_gainers_day,
                market_breadth_data=self.market_breadth_data,
                top_losers_day=self.top_losers_day,
                all_symbols=self.all_symbols,
                ac_api=self.ac_api,
            )
            await crypto_analytics.process_data(candles)

import logging
from datetime import datetime

from kafka import KafkaConsumer

from consumers.autotrade_consumer import AutotradeConsumer
from models.klines import KlineProduceModel
from producers.analytics import CryptoAnalytics
from shared.apis.binbot_api import BinanceApi, BinbotApi
from shared.apis.kucoin_api import KucoinApi
from shared.apis.types import CombinedApis
from shared.enums import BinanceKlineIntervals, ExchangeId, KucoinKlineIntervals
from shared.streaming.async_producer import AsyncProducer


class KlinesProvider:
    """
    Pools, processes, agregates and provides klines data
    """

    def __init__(self, consumer: KafkaConsumer) -> None:
        super().__init__()
        # If we don't instantiate separately, almost no messages are received
        self.binbot_api = BinbotApi()
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.api: CombinedApis
        if self.autotrade_settings["exchange_id"] == "kucoin":
            self.exchange = ExchangeId.KUCOIN
            self.api = KucoinApi()
            self.interval = KucoinKlineIntervals.FIFTEEN_MINUTES.value
        else:
            self.exchange = ExchangeId.BINANCE
            self.api = BinanceApi()
            self.interval = BinanceKlineIntervals.fifteen_minutes.value

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

    async def load_data_on_start(self):
        self.producer = await self.producer.start()
        # Klines API dependencies
        self.active_pairs = self.binbot_api.get_active_pairs()
        self.top_gainers_day = await self.binbot_api.get_top_gainers()
        self.top_losers_day = await self.binbot_api.get_top_losers()
        self.market_breadth_data = await self.binbot_api.get_market_breadth()

        # Autotrade Consumer API dependencies
        self.ac_api = AutotradeConsumer(
            autotrade_settings=self.binbot_api.get_autotrade_settings(),
            active_test_bots=self.binbot_api.get_active_pairs(
                collection_name="paper_trading"
            ),
            all_symbols=self.binbot_api.get_symbols(),
            # Active bot symbols substracting exchange active symbols (not blacklisted)
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
            data = payload
            klines = KlineProduceModel.model_validate(data)
            kucoin_symbol = klines.symbol
            symbol = kucoin_symbol.replace("-", "")
            kline_limit = 1000
            # Build time window for 1000 klines of 15min (or current interval)
            if self.exchange == ExchangeId.KUCOIN:
                candles = self.api.get_ui_klines(
                    kucoin_symbol,
                    interval=self.interval,
                    limit=kline_limit,
                )
            else:
                # Binance path supports limit directly
                candles = self.api.get_ui_klines(
                    symbol, interval=self.interval, limit=kline_limit
                )

            if len(candles) == 0:
                logging.warning(f"{symbol} No data to do analytics")
                return

            crypto_analytics = CryptoAnalytics(
                producer=self.producer,
                api=self.api,
                kucoin_symbol=kucoin_symbol,
                symbol=symbol,
                top_gainers_day=self.top_gainers_day,
                market_breadth_data=self.market_breadth_data,
                top_losers_day=self.top_losers_day,
                all_symbols=self.all_symbols,
                ac_api=self.ac_api,
                exchange=self.exchange,
            )
            await crypto_analytics.process_data(candles)

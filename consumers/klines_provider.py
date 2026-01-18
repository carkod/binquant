from datetime import datetime
from kafka import KafkaConsumer
from pybinbot import (
    BinanceKlineIntervals,
    ExchangeId,
    KucoinKlineIntervals,
    BinbotApi,
    KucoinApi,
    BinanceApi,
)
from consumers.autotrade_consumer import AutotradeConsumer
from models.klines import KlineProduceModel
from producers.analytics import CryptoAnalytics
from shared.streaming.async_producer import AsyncProducer
from shared.config import Config


class KlinesProvider:
    """
    Pools, processes, aggregates, and provides klines data.

    Maintains a rolling list of raw candles per symbol. Merges incoming
    WebSocket updates into historical data and passes it to CryptoAnalytics.
    """

    MAX_CANDLES = 400

    def __init__(self, consumer: KafkaConsumer) -> None:
        self.config = Config()
        self.binbot_api = BinbotApi()
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.api: KucoinApi | BinanceApi
        self.exchange: ExchangeId
        self.interval: BinanceKlineIntervals | KucoinKlineIntervals
        self.consumer = consumer
        self.producer: AsyncProducer = AsyncProducer()
        # Candles/btc candles storage
        self.candles: list = []
        self.btc_candles: list[list] = []

        # Determine exchange
        if self.autotrade_settings["exchange_id"] == "kucoin":
            self.exchange = ExchangeId.KUCOIN
            self.api = KucoinApi(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passphrase=self.config.kucoin_passphrase,
            )
            self.interval = KucoinKlineIntervals.FIFTEEN_MINUTES
        else:
            self.exchange = ExchangeId.BINANCE
            self.api = BinanceApi(
                key=self.config.binance_key, secret=self.config.binance_secret
            )
            self.interval = BinanceKlineIntervals.fifteen_minutes

        self.all_symbols = self.binbot_api.get_symbols()

        # Autotrade consumer setup
        self.ac_api = AutotradeConsumer(
            autotrade_settings=self.autotrade_settings,
            active_test_bots=self.binbot_api.get_active_pairs(
                collection_name="paper_trading"
            ),
            all_symbols=self.all_symbols,
            test_autotrade_settings=self.binbot_api.get_test_autotrade_settings(),
        )

    async def load_data_on_start(self):
        """Load initial BTC benchmark candles and market data."""
        self.producer = await self.producer.start()

        # Load market-level data
        self.active_pairs = self.binbot_api.get_active_pairs()
        self.top_gainers_day = await self.binbot_api.get_top_gainers()
        self.top_losers_day = await self.binbot_api.get_top_losers()
        self.market_breadth_data = await self.binbot_api.get_market_breadth()

        # Load BTC benchmark candles
        btc_symbol = "BTCUSDT" if self.exchange == ExchangeId.BINANCE else "BTC-USDT"
        self.btc_candles = self.api.get_ui_klines(
            symbol=btc_symbol,
            interval=self.interval.value,
            limit=self.MAX_CANDLES,
        )

    async def aggregate_data(self, payload: dict):
        """
        Merge new asset candle and pass data to CryptoAnalytics.
        """
        # Reload market data at the top of each hour
        current_time = datetime.now()
        if current_time.minute == 0:
            self.top_gainers_day = await self.binbot_api.get_top_gainers()
            self.top_losers_day = await self.binbot_api.get_top_losers()
            self.market_breadth_data = await self.binbot_api.get_market_breadth()

        # Convert payload into standardized candle dict
        klines = KlineProduceModel.model_validate(payload)
        kucoin_symbol = klines.symbol
        symbol = kucoin_symbol.replace("-", "")

        self.candles = self.api.get_ui_klines(
            symbol=kucoin_symbol if self.exchange == ExchangeId.KUCOIN else symbol,
            interval=self.interval.value,
            limit=self.MAX_CANDLES,
        )
        self.btc_candles = self.api.get_ui_klines(
            symbol="BTC-USDT" if self.exchange == ExchangeId.KUCOIN else "BTCUSDT",
            interval=self.interval.value,
            limit=self.MAX_CANDLES,
        )

        # Pass candles to CryptoAnalytics for processing
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
        await crypto_analytics.process_data(
            candles=self.candles,
            btc_candles=self.btc_candles,
        )

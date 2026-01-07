from datetime import datetime
from kafka import KafkaConsumer
from pybinbot import BinanceKlineIntervals, ExchangeId, KucoinKlineIntervals

from consumers.autotrade_consumer import AutotradeConsumer
from models.klines import KlineProduceModel
from producers.analytics import CryptoAnalytics
from shared.apis.binbot_api import BinbotApi
from shared.apis.kucoin_api import KucoinApi
from shared.apis.binance_api import BinanceApi
from shared.apis.types import CombinedApis
from shared.streaming.async_producer import AsyncProducer
from threading import Lock
from collections import defaultdict


class KlinesProvider:
    """
    Pools, processes, aggregates, and provides klines data.

    Maintains a rolling list of raw candles per symbol. Merges incoming
    WebSocket updates into historical data and passes it to CryptoAnalytics.
    """

    MAX_CANDLES = 400

    def __init__(self, consumer: KafkaConsumer) -> None:
        self.binbot_api = BinbotApi()
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.api: CombinedApis
        self.exchange: ExchangeId
        self.interval: str
        self.consumer = consumer
        self.producer: AsyncProducer = AsyncProducer()
        # per-symbol rolling raw candles
        self.asset_klines: dict[str, list[dict]] = {}
        self.btc_klines: list[dict] = []
        # Locks per symbol to prevent race conditions
        self.symbol_locks: dict[str, Lock] = defaultdict(Lock)

        # Determine exchange
        if self.autotrade_settings["exchange_id"] == "kucoin":
            self.exchange = ExchangeId.KUCOIN
            self.api = KucoinApi()
            self.interval = KucoinKlineIntervals.FIFTEEN_MINUTES.value
        else:
            self.exchange = ExchangeId.BINANCE
            self.api = BinanceApi()
            self.interval = BinanceKlineIntervals.fifteen_minutes.value

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
        self.btc_klines = self.api.get_ui_klines(
            symbol=btc_symbol,
            interval=self.interval,
            limit=self.MAX_CANDLES,
        )

    def update_candles(self, symbol: str, new_candle: dict):
        """
        Thread-safe update of raw candles from websocket
        """
        with self.symbol_locks[symbol]:
            self.asset_klines[symbol] = self.merge_candle(
                self.asset_klines[symbol], new_candle
            )

    def merge_candle(
        self,
        candles: list[dict],
        new_candle: dict,
        max_len: int = MAX_CANDLES,
    ) -> list[dict]:
        """
        Merge a new candle into a rolling list of raw candles.
        Replaces last candle if timestamps match, appends otherwise,
        and keeps at most `max_len` candles.
        """
        if not candles:
            return [new_candle]

        last_candle = candles[-1]

        if int(new_candle["open_time"]) == int(last_candle[0]):
            candles[-1] = new_candle  # update last candle
        elif int(new_candle["open_time"]) > int(last_candle[0]):
            candles.append(new_candle)
            if len(candles) > max_len:
                candles = candles[-max_len:]  # trim oldest candles

        return candles

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

        # Fetch historical candles once if not already cached
        if symbol not in self.asset_klines:
            historical_candles = self.api.get_ui_klines(
                symbol=kucoin_symbol if self.exchange == ExchangeId.KUCOIN else symbol,
                interval=self.interval,
                limit=self.MAX_CANDLES,
            )
            self.asset_klines[symbol] = historical_candles or []

        # Merge the new WebSocket candle
        self.update_candles(
            symbol,
            klines.model_dump(),
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
            candles=self.asset_klines[symbol],
            btc_candles=self.btc_klines,
        )

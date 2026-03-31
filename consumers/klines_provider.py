import logging
from datetime import datetime
from kafka import KafkaConsumer
from pybinbot import (
    BinanceKlineIntervals,
    ExchangeId,
    KucoinKlineIntervals,
    BinbotApi,
    KucoinApi,
    BinanceApi,
    AsyncProducer,
    KlineProduceModel,
    MarketType,
    KucoinFutures,
)
from consumers.autotrade_consumer import AutotradeConsumer
from producers.context_evaluator import ContextEvaluator
from shared.config import Config
from time import time


class KlinesProvider:
    """
    Pools, processes, aggregates, and provides klines data.

    Maintains a rolling list of raw candles per symbol. Merges incoming
    WebSocket updates into historical data and passes it to ContextEvaluator.
    """

    MAX_CANDLES = 400

    def __init__(self, consumer: KafkaConsumer) -> None:
        self.config = Config()
        self.binbot_api = BinbotApi(
            base_url=self.config.backend_domain,
            service_email=self.config.service_email,
            service_password=self.config.service_password,
        )
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.api: KucoinApi | BinanceApi | KucoinFutures
        self.kucoin_futures_api = KucoinFutures(
            key=self.config.kucoin_key,
            secret=self.config.kucoin_secret,
            passphrase=self.config.kucoin_passphrase,
        )
        self.exchange: ExchangeId
        self.interval: BinanceKlineIntervals | KucoinKlineIntervals
        self.consumer = consumer
        self.producer = AsyncProducer(
            host=self.config.kafka_host, port=self.config.kafka_port
        )
        # Apex Flow starting point for scoring signals
        self.first_seen_at = int(time() * 1000)
        # Candles/btc candles storage
        self.candles: list[list] = []
        self.btc_candles: list[list] = []
        # Open interest cache
        # symbol -> {timestamp: openInterest}
        self.oi_cache: dict[str, tuple[int, float]] = {}
        self.CACHE_TTL_MS = 5000

        # Determine exchange
        if self.autotrade_settings["exchange_id"] == "kucoin":
            self.exchange = ExchangeId.KUCOIN
            self.api = KucoinApi(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passphrase=self.config.kucoin_passphrase,
            )
            self.interval = KucoinKlineIntervals.FIVE_MINUTES
        else:
            self.exchange = ExchangeId.BINANCE
            self.api = BinanceApi(
                key=self.config.binance_key, secret=self.config.binance_secret
            )
            self.interval = BinanceKlineIntervals.five_minutes

        self.all_symbols = self.binbot_api.get_symbols()

        # Autotrade consumer setup
        self.ac_api = AutotradeConsumer(
            autotrade_settings=self.autotrade_settings,
            active_test_bots=self.binbot_api.get_active_pairs(
                collection_name="paper_trading"
            ),
            all_symbols=self.all_symbols,
            test_autotrade_settings=self.binbot_api.get_test_autotrade_settings(),
            binbot_api=self.binbot_api,
        )

    def _refresh_btc_candles(self) -> bool:
        """
        Refresh if interval exceeded since last BTC candle.
        """
        refresh_btc_candles = False
        if len(self.btc_candles) == 0:
            refresh_btc_candles = True
        else:
            last_btc_open_time = self.btc_candles[-1][0]  # open_time in ms
            now_ts = int(time() * 1000)
            if now_ts - last_btc_open_time > int(self.interval.get_ms()):
                refresh_btc_candles = True

        return refresh_btc_candles

    def retrieve_oi(self, kucoin_symbol: str) -> float:
        """
        Fetch current open interest from KuCoin with caching and compute OI growth.
        Returns a float: growth factor relative to last cached OI (1.0 if first tick or no change)
        """
        now_ms = int(time() * 1000)
        # Fetch fresh OI from API
        current_oi = float(self.kucoin_futures_api.get_open_interest(kucoin_symbol))

        if kucoin_symbol in self.oi_cache:
            last_ts, last_oi = self.oi_cache[kucoin_symbol]
            if now_ms - last_ts < self.CACHE_TTL_MS:
                current_oi = last_oi
        else:
            # Update cache
            self.oi_cache[kucoin_symbol] = (now_ms, current_oi)
            last_oi = 1.0

        oi_growth: float = max(current_oi / last_oi, 0.0)
        if last_oi is None or last_oi == 0:
            oi_growth = 1.0

        # Update last OI for next tick
        self.oi_cache[kucoin_symbol] = (now_ms, current_oi)
        return oi_growth

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
        Merge new asset candle and pass data to ContextEvaluator.
        - Reload market data at the top of each hour
        """
        current_time = datetime.now()
        if current_time.minute == 0:
            self.top_gainers_day = await self.binbot_api.get_top_gainers()
            self.top_losers_day = await self.binbot_api.get_top_losers()
            self.market_breadth_data = await self.binbot_api.get_market_breadth()

        # Convert payload into standardized candle dict
        klines = KlineProduceModel.model_validate(payload)
        if klines.market_type == MarketType.FUTURES:
            self.api = KucoinFutures(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passphrase=self.config.kucoin_passphrase,
            )

        kucoin_symbol = klines.symbol
        symbol = kucoin_symbol.replace("-", "")
        api_symbol = kucoin_symbol if self.exchange == ExchangeId.KUCOIN else symbol

        self.candles = self.api.get_ui_klines(
            symbol=api_symbol,
            interval=self.interval.value,
            limit=self.MAX_CANDLES,
        )
        # Refresh BTC candles if empty or last open time is more than 15 minutes behind
        refresh_btc_candles = self._refresh_btc_candles()

        if refresh_btc_candles:
            if klines.market_type == MarketType.FUTURES:
                self.btc_candles = self.api.get_ui_klines(
                    symbol="ETHBTCUSDTM",
                    interval=self.interval.value,
                    limit=self.MAX_CANDLES,
                )
            else:
                symbol = (
                    "BTCUSDT" if self.exchange == ExchangeId.BINANCE else "BTC-USDT"
                )
                self.btc_candles = self.api.get_ui_klines(
                    symbol=symbol,
                    interval=self.interval.value,
                    limit=self.MAX_CANDLES,
                )

        all_symbols = [s for s in self.all_symbols if s["id"] == symbol]
        if all_symbols and len(all_symbols) > 0:
            current_symbol_data = all_symbols[0]
        else:
            current_symbol_data = None
            # Can't work with a symbol that doesn't exist in our symbols table
            logging.error(f"Symbol {symbol} not found in symbols list. Skipping.")
            return

        # Pass candles to ContextEvaluator for processing
        crypto_analytics = ContextEvaluator(
            producer=self.producer,
            api=self.api,
            kucoin_symbol=kucoin_symbol,
            symbol=symbol,
            current_symbol_data=current_symbol_data,
            top_gainers_day=self.top_gainers_day,
            market_breadth_data=self.market_breadth_data,
            top_losers_day=self.top_losers_day,
            all_symbols=self.all_symbols,
            ac_api=self.ac_api,
            exchange=self.exchange,
            first_seen_at=self.first_seen_at,
            interval=self.interval,
            market_type=klines.market_type if klines.market_type else MarketType.SPOT,
            oi_data=self.retrieve_oi(kucoin_symbol),
            binbot_api=self.binbot_api,
        )
        await crypto_analytics.process_data(
            candles=self.candles,
            btc_candles=self.btc_candles,
        )

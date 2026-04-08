import logging
from datetime import datetime
from kafka import KafkaConsumer
from pandas import DataFrame
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
from market_regime_prediction.live_market_context_accumulator import (
    LiveMarketContextAccumulator,
)
from market_regime_prediction.models import LiveMarketContext
from market_regime_prediction.market_state_store import MarketStateStore
from producers.context_evaluator import ContextEvaluator
from shared.config import Config
from time import time


class KlinesProvider:
    """
    Pools, processes, aggregates, and provides klines data.

    Maintains a rolling list of raw candles per symbol. Merges incoming
    WebSocket updates into historical data and passes it to ContextEvaluator.
    """

    LIMIT = 400

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
        self.interval_15m: BinanceKlineIntervals | KucoinKlineIntervals
        self.consumer = consumer
        self.producer = AsyncProducer(
            host=self.config.kafka_host, port=self.config.kafka_port
        )
        # Apex Flow starting point for scoring signals
        self.first_seen_at = int(time() * 1000)
        # Candles/btc candles storage
        self.candles: list[list] = []
        self.candles_15m: list[list] = []
        self.btc_candles_15m: list[list] = []
        self.market_state_store = MarketStateStore(max_bars_per_symbol=self.LIMIT)
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
            self.interval_15m = KucoinKlineIntervals.FIFTEEN_MINUTES
            self.benchmark_symbol = "BTC-USDT"
            self.futures_benchmark_symbol = "XBTUSDTM"
        else:
            self.exchange = ExchangeId.BINANCE
            self.api = BinanceApi(
                key=self.config.binance_key, secret=self.config.binance_secret
            )
            self.interval = BinanceKlineIntervals.five_minutes
            self.interval_15m = BinanceKlineIntervals.fifteen_minutes
            self.benchmark_symbol = "BTCUSDC"
            self.futures_benchmark_symbol = "BTCUSDTM"

        self.market_context_accumulator = LiveMarketContextAccumulator(
            state_store=self.market_state_store,
            btc_symbol=self._normalize_store_symbol(self.futures_benchmark_symbol),
        )
        self.latest_market_context: LiveMarketContext | None = None

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

    @staticmethod
    def _normalize_store_symbol(symbol: str) -> str:
        return symbol.replace("-", "").strip().upper()

    def _get_benchmark_symbol(self, market_type: MarketType = MarketType.SPOT) -> str:
        if market_type == MarketType.FUTURES:
            return self.futures_benchmark_symbol
        return self.benchmark_symbol

    @classmethod
    def _raw_kline_to_store_candle(cls, kline: list) -> dict | None:
        """
        Convert raw UI kline rows into the store's normalized candle format.

        The APIs in this repo consistently expose the first 6-7 columns as:
        open_time, open, high, low, close, volume, close_time.
        """
        if len(kline) < 6:
            return None

        close_time = kline[6] if len(kline) > 6 else kline[0]
        return {
            "timestamp": close_time,
            "open": kline[1],
            "high": kline[2],
            "low": kline[3],
            "close": kline[4],
            "volume": kline[5],
        }

    def _sync_market_state_from_ui_klines(
        self, symbol: str, ui_klines: list[list]
    ) -> None:
        rows = []
        for raw_kline in ui_klines:
            candle = self._raw_kline_to_store_candle(raw_kline)
            if candle is not None:
                rows.append(candle)

        if not rows:
            return

        self.market_state_store.update(
            symbol=self._normalize_store_symbol(symbol),
            candle=DataFrame(rows),
        )

    def _store_btc_history(self, market_type: MarketType) -> None:
        btc_symbol = self._get_benchmark_symbol(market_type)
        self._sync_market_state_from_ui_klines(
            symbol=btc_symbol,
            ui_klines=self.btc_candles_15m,
        )

    def _refresh_symbol_histories(
        self,
        api_symbol: str,
        market_type: MarketType,
    ) -> None:
        self.candles = self.api.get_ui_klines(
            symbol=api_symbol,
            interval=self.interval.value,
            limit=self.LIMIT,
        )
        self.candles_15m = self.api.get_ui_klines(
            symbol=api_symbol,
            interval=self.interval_15m.value,
            limit=self.LIMIT,
        )
        self._refresh_btc_candles_15m(market_type)
        self._sync_market_state_from_ui_klines(
            symbol=api_symbol,
            ui_klines=self.candles_15m,
        )
        self._store_btc_history(market_type=market_type)
        if self.candles_15m:
            latest_candle = self._raw_kline_to_store_candle(self.candles_15m[-1])
            if latest_candle is not None:
                self.market_context_accumulator.btc_symbol = (
                    self._normalize_store_symbol(
                        self._get_benchmark_symbol(market_type)
                    )
                )
                self.latest_market_context = (
                    self.market_context_accumulator.refresh_context_for_timestamp(
                        int(latest_candle["timestamp"])
                    )
                )

    def _refresh_btc_candles_15m(self, market_type: MarketType) -> None:
        """
        Refresh if interval exceeded since last BTC candle.
        """
        if len(self.btc_candles_15m) == 0:
            refresh_btc_candles = True
        else:
            last_btc_open_time = self.btc_candles_15m[-1][0]  # open_time in ms
            now_ts = int(time() * 1000)
            refresh_btc_candles = now_ts - last_btc_open_time > int(
                self.interval_15m.get_ms()
            )

        if refresh_btc_candles:
            self.btc_candles_15m = self.api.get_ui_klines(
                symbol=self._get_benchmark_symbol(market_type),
                interval=self.interval_15m.value,
                limit=self.LIMIT,
            )

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
        self.btc_candles_15m = self.api.get_ui_klines(
            symbol=self._get_benchmark_symbol(MarketType.SPOT),
            interval=self.interval_15m.value,
            limit=self.LIMIT,
        )
        self._store_btc_history(MarketType.SPOT)

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
        market_type = klines.market_type or MarketType.SPOT

        kucoin_symbol = klines.symbol
        symbol = kucoin_symbol.replace("-", "")
        api_symbol = kucoin_symbol if self.exchange == ExchangeId.KUCOIN else symbol

        self._refresh_symbol_histories(api_symbol=api_symbol, market_type=market_type)

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
            market_type=market_type,
            oi_data=self.retrieve_oi(kucoin_symbol),
            latest_market_context=self.latest_market_context,
            binbot_api=self.binbot_api,
        )
        await crypto_analytics.process_data(
            candles=self.candles,
            candles_15m=self.candles_15m,
            btc_candles_15m=self.btc_candles_15m,
        )

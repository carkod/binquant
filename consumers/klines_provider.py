import logging
from datetime import datetime
from pandas import DataFrame
from pybinbot import (
    BinanceKlineIntervals,
    ExchangeId,
    KucoinKlineIntervals,
    BinbotApi,
    KucoinApi,
    BinanceApi,
    KlineProduceModel,
    MarketType,
    KucoinFutures,
    SymbolModel,
    MarketBreadthSeries,
    GainersLosersSnapshot,
)
from calibrators.leverage_calibrator import LeverageCalibrator
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from market_regime.live_market_context_accumulator import (
    LiveMarketContextAccumulator,
)
from market_regime.derivatives_positioning import DerivativesPositioningProvider
from market_regime.liquidation_state_store import LiquidationStateStore
from market_regime.models import LiveMarketContext
from market_regime.market_state_store import MarketStateStore
from producers.context_evaluator import ContextEvaluator
from shared.config import Config
from strategies.liquidation_sweep_pump import LiquidationSweepPortfolioSelector
from time import time


class KlinesProvider:
    """
    Pools, processes, aggregates, and provides klines data.

    Maintains a rolling list of raw candles per symbol. Merges incoming
    WebSocket updates into historical data and passes it to ContextEvaluator.
    """

    LIMIT = 400

    def __init__(
        self,
        liquidation_store: LiquidationStateStore | None = None,
    ) -> None:
        self.config = Config()
        self.binbot_api = BinbotApi(
            base_url=self.config.backend_domain,
            service_email=self.config.service_email,
            service_password=self.config.service_password,
        )
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.api: KucoinApi | BinanceApi | KucoinFutures
        self.exchange: ExchangeId
        self.interval: BinanceKlineIntervals | KucoinKlineIntervals
        self.interval_15m: BinanceKlineIntervals | KucoinKlineIntervals
        # Apex Flow starting point for scoring signals
        self.first_seen_at = int(time() * 1000)
        # Candles/btc candles storage
        self.candles: list[list] = []
        self.candles_15m: list[list] = []
        self.btc_candles_15m: list[list] = []
        self.market_state_store = MarketStateStore(max_bars_per_symbol=self.LIMIT)
        self.market_breadth_data: MarketBreadthSeries | None = None
        self.gainers_losers_series: list[GainersLosersSnapshot] = []
        self.liquidation_store = liquidation_store or LiquidationStateStore()
        self.kucoin_futures_api = KucoinFutures(
            key=self.config.kucoin_key,
            secret=self.config.kucoin_secret,
            passphrase=self.config.kucoin_passphrase,
        )
        self.binance_api = BinanceApi(
            key=self.config.binance_key,
            secret=self.config.binance_secret,
        )
        self.derivatives_positioning_provider = DerivativesPositioningProvider(
            kucoin_futures_api=self.kucoin_futures_api,
            binance_api=self.binance_api,
            liquidation_store=self.liquidation_store,
        )
        self.telegram_consumer = TelegramConsumer(
            token=self.config.telegram_bot_key,
            chat_id=self.config.telegram_user_id,
            is_enabled=self.autotrade_settings.telegram_signals,
        )
        self.strategy_cooldowns: dict[tuple[str, str], int] = {}
        self.strategy_states: dict[tuple[str, str], dict[str, float | int]] = {}
        self.liquidation_sweep_portfolio_selector = LiquidationSweepPortfolioSelector()

        # Determine exchange
        if self.autotrade_settings.exchange_id == "kucoin":
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
            self.api = self.binance_api
            self.interval = BinanceKlineIntervals.five_minutes
            self.interval_15m = BinanceKlineIntervals.fifteen_minutes
            self.benchmark_symbol = "BTCUSDC"
            self.futures_benchmark_symbol = "BTCUSDTM"

        self.market_context_accumulator = LiveMarketContextAccumulator(
            state_store=self.market_state_store,
            btc_symbol=self.futures_benchmark_symbol,
        )
        self.latest_market_context: LiveMarketContext | None = None
        self.last_market_regime: str | None = None

        self.all_symbols: list[SymbolModel] = self.binbot_api.get_symbols()

        # Autotrade consumer setup
        self.ac_api = AutotradeConsumer(
            autotrade_settings=self.autotrade_settings,
            active_test_bots=self.binbot_api.get_active_pairs(
                collection_name="paper_trading"
            ),
            all_symbols=self.all_symbols,
            test_autotrade_settings=self.binbot_api.get_test_autotrade_settings(),
            active_grid_ladders=self.binbot_api.get_active_grid_ladders(),
            binbot_api=self.binbot_api,
        )

        self.leverage_calibrator = LeverageCalibrator(
            binbot_api=self.binbot_api, exchange=self.exchange
        )
        self._last_calibration_bucket: int | None = None
        self._last_market_tape_bucket: int | None = None

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
    ) -> list[dict]:
        rows = []
        for raw_kline in ui_klines:
            candle = self._raw_kline_to_store_candle(raw_kline)
            if candle is not None and int(candle["timestamp"]) <= int(time() * 1000):
                rows.append(candle)

        if not rows:
            return []

        self.market_state_store.update(
            symbol=symbol,
            candle=DataFrame(rows),
        )
        return rows

    def _store_btc_history(self, market_type: MarketType) -> None:
        btc_symbol = self._get_benchmark_symbol(market_type)
        self._sync_market_state_from_ui_klines(
            symbol=btc_symbol,
            ui_klines=self.btc_candles_15m,
        )

    def _refresh_latest_market_context(
        self,
        *,
        timestamp: int,
        market_type: MarketType,
    ) -> LiveMarketContext | None:
        self.market_context_accumulator.btc_symbol = self._get_benchmark_symbol(
            market_type
        )
        context = self.market_context_accumulator.refresh_context_for_timestamp(
            timestamp
        )
        if context is not None:
            self.latest_market_context = context
        elif self.latest_market_context is None:
            self.latest_market_context = (
                self.market_context_accumulator.get_latest_context()
            )
        return self.latest_market_context

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
        closed_symbol_candles = self._sync_market_state_from_ui_klines(
            symbol=api_symbol,
            ui_klines=self.candles_15m,
        )
        self._store_btc_history(market_type=market_type)
        if market_type == MarketType.FUTURES:
            positioning = self.derivatives_positioning_provider.get_positioning(
                api_symbol,
                self.candles_15m,
            )
            self.market_context_accumulator.update_derivatives_positioning(
                api_symbol,
                positioning,
            )
        if closed_symbol_candles:
            latest_candle = closed_symbol_candles[-1]
            self._refresh_latest_market_context(
                timestamp=int(latest_candle["timestamp"]),
                market_type=market_type,
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

    async def _refresh_market_tape_for_bucket(self, current_time: datetime) -> None:
        """
        Reload the market-wide tape feeds once per 15m bucket.

        The gainers/losers snapshots are only ingested hourly upstream, so a
        15m cadence re-reads the same snapshot a few times rather than missing
        one, which is the cheaper failure of the two.
        """
        bucket = int(current_time.timestamp() * 1000 // self.interval_15m.get_ms())
        if bucket == self._last_market_tape_bucket:
            return

        await self._refresh_market_tape()
        self._last_market_tape_bucket = bucket

    async def _refresh_market_tape(self) -> None:
        """Refresh independent market feeds without invalidating cached data."""
        try:
            self.market_breadth_data = await self.binbot_api.get_market_breadth()
        except Exception:
            logging.exception(
                "Market breadth refresh failed; retaining the previous snapshot."
            )

        try:
            self.gainers_losers_series = (
                await self.binbot_api.get_gainers_losers_series()
            )
        except Exception:
            logging.exception(
                "Gainers/losers series refresh failed; retaining the previous series."
            )

    async def load_data_on_start(self):
        """Load initial BTC benchmark candles and market data."""
        # Load market-level data
        self.active_pairs = self.binbot_api.get_active_pairs()
        await self._refresh_market_tape()
        self._last_market_tape_bucket = int(
            datetime.now().timestamp() * 1000 // self.interval_15m.get_ms()
        )

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
        - Reload market breadth and gainers/losers once per 15-minute bucket
        """
        current_time = datetime.now()
        await self._refresh_market_tape_for_bucket(current_time)

        # Recalibrate per-symbol futures_leverage on each 15m boundary, but
        # only once per bucket so multiple kline payloads in the same minute
        # don't trigger duplicate PUT cycles.
        bucket = int(current_time.timestamp() // (15 * 60))
        if (
            bucket != self._last_calibration_bucket
            and self.latest_market_context is not None
        ):
            self._last_calibration_bucket = bucket
            try:
                self.leverage_calibrator.calibrate_all(
                    self.latest_market_context, self.all_symbols
                )
            except Exception:
                logging.exception(
                    "[LeverageCalibrator] cycle failed; continuing kline processing."
                )

        # Convert payload into standardized candle dict
        klines = KlineProduceModel.model_validate(payload)
        if klines.market_type == MarketType.FUTURES:
            self.api = self.kucoin_futures_api
        market_type = klines.market_type or MarketType.SPOT

        kucoin_symbol = klines.symbol
        symbol = kucoin_symbol.replace("-", "")
        api_symbol = kucoin_symbol if self.exchange == ExchangeId.KUCOIN else symbol

        self._refresh_symbol_histories(api_symbol=api_symbol, market_type=market_type)

        current_symbol_data = next(
            (s for s in self.all_symbols if s.id == symbol), None
        )
        if current_symbol_data is None:
            # Can't work with a symbol that doesn't exist in our symbols table
            logging.error(f"Symbol {symbol} not found in symbols list. Skipping.")
            return

        # Pass candles to ContextEvaluator for processing
        crypto_analytics = ContextEvaluator(
            api=self.api,
            kucoin_symbol=kucoin_symbol,
            symbol=symbol,
            current_symbol_data=current_symbol_data,
            market_breadth_data=self.market_breadth_data,
            gainers_losers_series=self.gainers_losers_series,
            all_symbols=self.all_symbols,
            ac_api=self.ac_api,
            exchange=self.exchange,
            first_seen_at=self.first_seen_at,
            interval=self.interval,
            market_type=market_type,
            latest_market_context=self.latest_market_context,
            binbot_api=self.binbot_api,
            last_market_regime=self.last_market_regime,
            telegram_consumer=self.telegram_consumer,
            strategy_cooldowns=self.strategy_cooldowns,
            strategy_states=self.strategy_states,
            liquidation_sweep_portfolio_selector=(
                self.liquidation_sweep_portfolio_selector
            ),
        )
        await crypto_analytics.process_data(
            candles=self.candles,
            candles_15m=self.candles_15m,
            btc_candles_15m=self.btc_candles_15m,
        )
        self.last_market_regime = crypto_analytics.last_market_regime

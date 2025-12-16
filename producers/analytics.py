from datetime import datetime

from confluent_kafka import Producer
from pandas import DataFrame, to_datetime

from algorithms.market_breadth import MarketBreadthAlgo
from algorithms.spike_hunter_v2 import SpikeHunterV2
from algorithms.spike_hunter_v3_kucoin import SpikeHunterV3KuCoin
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from models.signals import HABollinguerSpread
from shared.apis.binbot_api import BinbotApi
from shared.apis.types import CombinedApis
from shared.enums import BinanceKlineIntervals, ExchangeId, MarketDominance, Strategy
from shared.heikin_ashi import HeikinAshi
from shared.indicators import Indicators
from shared.utils import round_numbers


class CryptoAnalytics:
    def __init__(
        self,
        producer: Producer,
        api: CombinedApis,
        symbol,
        top_gainers_day,
        market_breadth_data,
        top_losers_day,
        all_symbols,
        ac_api,
        exchange,
    ) -> None:
        """
        Only variables no data requests (third party or db)
        or pipeline instances

        Network requested data that doesn't require reloading/real-time/updating
        should be on klines_provider instance
        """
        self.producer = producer
        self.api = api
        self.binbot_api = BinbotApi()
        self.symbol = symbol
        self.price_precision = self.binbot_api.price_precision(
            symbol=symbol.replace("-", "")
        )
        self.df = DataFrame()
        self.df_4h = DataFrame()
        self.df_1h = DataFrame()
        self.ha_df = DataFrame()
        self.interval = BinanceKlineIntervals.fifteen_minutes.value
        self.exchange = exchange
        # describes current USDC market: gainers vs losers
        self.current_market_dominance: MarketDominance = MarketDominance.NEUTRAL
        # describes whether tide is shifting
        self.market_domination_reversal: bool = False
        self.bot_strategy: Strategy = Strategy.long
        self.top_coins_gainers: list[str] = []
        self.top_gainers_day = top_gainers_day
        self.top_losers_day = top_losers_day
        self.market_breadth_data = market_breadth_data
        self.btc_correlation: float = 0
        self.btc_price: float = 0.0
        self.repeated_signals: dict = {}
        self.all_symbols = all_symbols
        self.current_symbol_data: dict | None = next(
            (s for s in all_symbols if s["id"] == symbol), None
        )
        self.telegram_consumer = TelegramConsumer()
        self.at_consumer: AutotradeConsumer = ac_api

    def days(self, secs):
        return secs * 86400

    def bb_spreads(self) -> HABollinguerSpread:
        """
        Calculate Heikin Ashi Bollinguer bands spreads for trailling strategies
        """
        bb_high = float(self.df.bb_upper.iloc[-1])
        bb_mid = float(self.df.bb_mid.iloc[-1])
        bb_low = float(self.df.bb_lower.iloc[-1])
        return HABollinguerSpread(
            bb_high=round_numbers(bb_high, 6),
            bb_mid=round_numbers(bb_mid, 6),
            bb_low=round_numbers(bb_low, 6),
        )

    def preprocess_data(self, candles):
        # Pre-process
        self.df = DataFrame(candles)
        if self.exchange == ExchangeId.BINANCE:
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
                [
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
                ]
            ]
        else:
            self.df.columns = [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
            ]
            self.df = self.df[
                [
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                ]
            ]

        # Convert price and volume columns to float
        price_volume_columns = ["open", "high", "low", "close", "volume"]
        self.df[price_volume_columns] = self.df[price_volume_columns].astype(float)

        # # Generate Heikin Ashi DataFrame once processed and cleaned
        self.df = HeikinAshi.get_heikin_ashi(self.df)

        # Ensure close_time is datetime and set as index for proper resampling
        self.df["timestamp"] = to_datetime(self.df["close_time"], unit="ms")
        self.df.set_index("timestamp", inplace=True)

        # Sort by timestamp ascending (required for rolling windows and resampling)
        self.df = self.df.sort_index()

        # Remove duplicate timestamps, keeping the last occurrence
        self.df = self.df[~self.df.index.duplicated(keep="last")]

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

        # Keep a copy of a clean df
        # Some algos don't need indicators
        self.clean_df = self.df.copy()

    def postprocess_data(self):
        """
        Post-process the data after all indicators have been applied.
        """

        # Drop any rows with NaN values
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)
        self.df_1h.dropna(inplace=True)
        self.df_1h.reset_index(drop=True, inplace=True)
        self.df_4h.dropna(inplace=True)
        self.df_4h.reset_index(drop=True, inplace=True)

        self.mda = MarketBreadthAlgo(cls=self)
        self.sh2 = SpikeHunterV2(cls=self)
        self.sh3 = SpikeHunterV3KuCoin(cls=self)

    async def process_data(self, candles):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        self.preprocess_data(candles)

        # self.df is the smallest interval, so this condition should cover resampled DFs as well as Heikin Ashi DF
        if self.df.empty is False and self.df.close.size > 0:
            # Basic technical indicators
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.df = Indicators.moving_averages(self.df, 7)
            self.df = Indicators.moving_averages(self.df, 25)
            self.df = Indicators.moving_averages(self.df, 100)

            # Oscillators
            self.df = Indicators.macd(self.df)
            self.df = Indicators.rsi(df=self.df)

            # Advanced technicals
            self.df = Indicators.ma_spreads(self.df)
            self.df = Indicators.bollinguer_spreads(self.df)
            self.df = Indicators.set_twap(self.df)

            self.postprocess_data()

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df["ma_7"].size < 7
                or self.df["ma_25"].size < 25
                or self.df["ma_100"].size < 100
            ):
                return

            close_price = float(self.df["close"].iloc[-1])

            spreads = self.bb_spreads()

            if not self.market_breadth_data or datetime.now().minute % 30 == 0:
                self.market_breadth_data = await self.binbot_api.get_market_breadth()

            await self.sh3.signal(
                current_price=close_price,
                bb_high=spreads.bb_high,
                bb_mid=spreads.bb_mid,
                bb_low=spreads.bb_low,
            )

        return

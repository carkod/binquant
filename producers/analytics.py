from datetime import datetime, timedelta

from confluent_kafka import Producer
from pandas import DataFrame, to_datetime

from algorithms.binance_report_ai import BinanceAIReport
from algorithms.coinrule import Coinrule
from algorithms.market_breadth import MarketBreadthAlgo
from algorithms.spike_hunter_v2 import SpikeHunterV2
from algorithms.spikehunter_v1 import SpikeHunter
from algorithms.whale_signals import WhaleSignals
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from models.signals import HABollinguerSpread
from shared.apis.binbot_api import BinbotApi
from shared.enums import BinanceKlineIntervals, MarketDominance, Strategy
from shared.heikin_ashi import HeikinAshi
from shared.indicators import Indicators
from shared.utils import round_numbers


class CryptoAnalytics:
    def __init__(
        self,
        producer: Producer,
        binbot_api: BinbotApi,
        symbol,
        top_gainers_day,
        market_breadth_data,
        top_losers_day,
        all_symbols,
        ac_api,
    ) -> None:
        """
        Only variables no data requests (third party or db)
        or pipeline instances

        Network requested data that doesn't require reloading/real-time/updating
        should be on klines_provider instance
        """
        self.producer = producer
        self.binbot_api = binbot_api
        self.symbol = symbol
        self.df = DataFrame()
        self.df_4h = DataFrame()
        self.df_1h = DataFrame()
        self.ha_df = DataFrame()
        self.interval = BinanceKlineIntervals.fifteen_minutes.value
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
        self.active_symbols = []
        self.current_symbol_data: dict | None = None
        for s in self.all_symbols:
            if s["active"]:
                self.active_symbols.append(s["id"])

            if s["id"] == symbol:
                self.current_symbol_data = s

        self.telegram_consumer = TelegramConsumer()
        self.at_consumer: AutotradeConsumer = ac_api

    def days(self, secs):
        return secs * 86400

    def bb_spreads(self) -> tuple[float, float, float]:
        """
        Calculate Bollinguer bands spreads for trailling strategies
        """
        bb_high = float(self.df.bb_upper.iloc[-1])
        bb_mid = float(self.df.bb_mid.iloc[-1])
        bb_low = float(self.df.bb_lower.iloc[-1])
        return (
            round_numbers(bb_high, 6),
            round_numbers(bb_mid, 6),
            round_numbers(bb_low, 6),
        )

    def ha_bb_spreads(self) -> HABollinguerSpread:
        """
        Calculate Heikin Ashi Bollinguer bands spreads for trailling strategies
        """
        ha_bb_high = float(self.ha_df.bb_upper.iloc[-1])
        ha_bb_mid = float(self.ha_df.bb_mid.iloc[-1])
        ha_bb_low = float(self.ha_df.bb_lower.iloc[-1])
        return HABollinguerSpread(
            ha_bb_high=round_numbers(ha_bb_high, 6),
            ha_bb_mid=round_numbers(ha_bb_mid, 6),
            ha_bb_low=round_numbers(ha_bb_low, 6),
        )

    def preprocess_data(self, candles):
        # Pre-process
        self.df = DataFrame(candles)
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

        # Convert price and volume columns to float
        price_volume_columns = ["open", "high", "low", "close", "volume"]
        self.df[price_volume_columns] = self.df[price_volume_columns].astype(float)

        # Generate Heikin Ashi DataFrame once processed and cleaned
        self.ha_df = HeikinAshi.get_heikin_ashi(self.df.copy())

        # Ensure close_time is datetime and set as index for proper resampling
        self.df["timestamp"] = to_datetime(self.df["close_time"], unit="ms")
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
        self.sh = SpikeHunter(cls=self)
        self.cr = Coinrule(cls=self)
        self.bar = BinanceAIReport(cls=self)
        self.sh2 = SpikeHunterV2(cls=self)
        self.whale = WhaleSignals(cls=self)

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

            # Heikin Ashi technicals
            self.ha_df = Indicators.bollinguer_spreads(self.ha_df)

            self.postprocess_data()

            # Avoid repeated signals in short periods of time
            is_lapsed = self.symbol in self.repeated_signals and self.repeated_signals[
                self.symbol
            ] < datetime.now() - timedelta(minutes=30)

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df["ma_7"].size < 7
                or self.df["ma_25"].size < 25
                or self.df["ma_100"].size < 100
                and not is_lapsed
            ):
                return

            if self.bot_strategy == Strategy.margin_short:
                return

            close_price = float(self.df["close"].iloc[-1])

            if self.btc_correlation == 0 or self.btc_price == 0:
                self.btc_correlation, self.btc_price = (
                    self.binbot_api.get_btc_correlation(symbol=self.symbol)
                )

            bb_high, bb_mid, bb_low = self.bb_spreads()
            ha_spreads = self.ha_bb_spreads()

            if not self.market_breadth_data or datetime.now().minute % 30 == 0:
                self.market_breadth_data = await self.binbot_api.get_market_breadth()

            # await self.mda.signal(
            #     close_price=close_price, bb_high=bb_high, bb_low=bb_low, bb_mid=bb_mid
            # )

            # emitted = await self.shm.signal(
            #     current_price=close_price,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

            # if not emitted:
            #     await self.sh.spike_hunter_bullish(
            #         current_price=close_price,
            #         bb_high=bb_high,
            #         bb_low=bb_low,
            #         bb_mid=bb_mid,
            #     )

            #     await self.sh.spike_hunter_breakouts(
            #         current_price=close_price,
            #         bb_high=bb_high,
            #         bb_low=bb_low,
            #         bb_mid=bb_mid,
            #     )

            await self.sh2.signal(
                current_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
                ha_bb_high=ha_spreads.ha_bb_high,
                ha_bb_mid=ha_spreads.ha_bb_mid,
                ha_bb_low=ha_spreads.ha_bb_low,
            )

            await self.bar.signal(
                current_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )

            # await self.cr.supertrend_swing_reversal(
            #     close_price=close_price,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

            # await local_min_max(
            #     df=self.df,
            #     current_price=close_price,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            #     symbol=self.symbol,
            #     telegram=self.telegram_consumer,
            #     at_consumer=self.at_consumer,
            #     precision=self.current_symbol_data["price_precision"]
            #     if self.current_symbol_data
            #     else 2,
            # )

            # await self.whale.signal(
            #     current_price=close_price,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

            # avoid repeating signals in short periods of time
            self.repeated_signals[self.symbol] = datetime.now()

        return

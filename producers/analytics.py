from datetime import datetime, timedelta

import pandas
from confluent_kafka import Producer
from pandas import DataFrame, to_datetime

from algorithms.atr_breakout import ATRBreakout
from algorithms.coinrule import Coinrule
from algorithms.heikin_ashi_spike_hunter import HASpikeHunter
from algorithms.local_min_max import local_min_max
from algorithms.market_breadth import MarketBreadthAlgo
from algorithms.spikehunter_v1 import SpikeHunter
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import BinanceKlineIntervals, MarketDominance, Strategy
from shared.indicators import Indicators
from shared.utils import round_numbers


class CryptoAnalytics:
    def __init__(
        self,
        producer: Producer,
        binbot_api: BinbotApi,
        df: pandas.DataFrame,
        symbol,
        df_4h,
        df_1h,
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
        self.df = df
        self.df_4h = df_4h
        self.df_1h = df_1h
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
        self.active_symbols = [s["id"] for s in self.all_symbols if s["active"]]

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
            ["open_time", "open", "high", "low", "close", "volume", "close_time"]
        ]

        # Convert price and volume columns to float
        price_volume_columns = ["open", "high", "low", "close", "volume"]
        self.df[price_volume_columns] = self.df[price_volume_columns].astype(float)

        # Ensure close_time is datetime and set as index for proper resampling
        self.df["timestamp"] = to_datetime(self.df["close_time"])
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

        # some algos don't need all these
        self.raw_df = self.df.copy()

        self.mda = MarketBreadthAlgo(cls=self)
        self.sh = SpikeHunter(cls=self)
        self.atr = ATRBreakout(cls=self)
        self.ha_sh = HASpikeHunter(cls=self)
        self.cr = Coinrule(cls=self)

    async def process_data(self, candles):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        self.preprocess_data(candles)

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

            if not self.market_breadth_data or datetime.now().minute % 30 == 0:
                self.market_breadth_data = await self.binbot_api.get_market_breadth()

            await self.mda.signal(
                close_price=close_price, bb_high=bb_high, bb_low=bb_low, bb_mid=bb_mid
            )

            emitted = await self.sh.spike_hunter_bullish(
                current_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )

            emitted = await self.sh.spike_hunter_breakouts(
                current_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )

            if not emitted:
                await self.sh.spike_hunter_standard(
                    current_price=close_price,
                    bb_high=bb_high,
                    bb_low=bb_low,
                    bb_mid=bb_mid,
                )

            await self.atr.atr_breakout(bb_high=bb_high, bb_low=bb_low, bb_mid=bb_mid)
            await self.atr.reverse_atr_breakout(
                bb_high=bb_high, bb_low=bb_low, bb_mid=bb_mid
            )

            await self.ha_sh.ha_spike_hunter_standard(
                current_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )

            await self.cr.supertrend_swing_reversal(
                close_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )

            await self.cr.buy_low_sell_high(
                close_price=close_price,
                rsi=self.df["rsi"].iloc[-1],
                ma_25=self.df["ma_25"].iloc[-1],
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            )

            await local_min_max(
                df=self.df,
                current_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
                symbol=self.symbol,
                telegram=self.telegram_consumer,
                at_consumer=self.at_consumer,
            )

            # avoid repeating signals in short periods of time
            self.repeated_signals[self.symbol] = datetime.now()

        return

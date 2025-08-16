from datetime import datetime, timedelta

import pandas
from aiokafka import AIOKafkaProducer
from pandas import Series

from algorithms.atr_breakout import ATRBreakout
from algorithms.market_breadth import MarketBreadthAlgo
from algorithms.spikehunter_v1 import SpikeHunter
from algorithms.top_gainer_drop import top_gainers_drop
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import BinanceKlineIntervals, MarketDominance, Strategy
from shared.utils import round_numbers


class TechnicalIndicators:
    def __init__(
        self,
        producer: AIOKafkaProducer,
        binbot_api: BinbotApi,
        df: pandas.DataFrame,
        symbol,
        df_4h,
        df_1h,
        top_gainers_day,
        market_breadth_data,
        top_losers_day,
    ) -> None:
        """
        Only variables
        no data requests (third party or db)
        or pipeline instances
        That will cause a lot of network requests
        """
        self.producer = producer
        self.binbot_api = binbot_api
        self.df = df
        self.symbol = symbol
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
        # Pre-initialize Market Breadth algorithm
        # because we don't need to load model every time
        self.mda = MarketBreadthAlgo(cls=self)
        self.sh = SpikeHunter(cls=self)
        self.atr = ATRBreakout(cls=self)
        self.btc_correlation: float = 0
        self.btc_price: float = 0.0
        self.repeated_signals: dict = {}
        self.all_symbols = self.binbot_api.get_symbols()
        self.active_symbols = [s["id"] for s in self.all_symbols if s["active"]]

        self.telegram_consumer = TelegramConsumer()
        self.at_consumer = AutotradeConsumer()

    def days(self, secs):
        return secs * 86400

    def bb_spreads(self) -> tuple[float, float, float]:
        """
        Calculate Bollinguer bands spreads for trailling strategies
        """

        bb_high = float(self.df.bb_upper[len(self.df.bb_upper) - 1])
        bb_mid = float(self.df.bb_mid[len(self.df.bb_mid) - 1])
        bb_low = float(self.df.bb_lower[len(self.df.bb_lower) - 1])

        return (
            round_numbers(bb_high, 6),
            round_numbers(bb_mid, 6),
            round_numbers(bb_low, 6),
        )

    def moving_averages(self, period=7):
        """
        Calculate moving averages for 7, 25, 100 days
        this also takes care of Bollinguer bands
        """
        self.df[f"ma_{period}"] = self.df["close"].rolling(window=period).mean()

    def macd(self):
        """
        Moving Average Convergence Divergence (MACD) indicator
        https://www.alpharithms.com/calculate-macd-python-272222/
        """

        k = self.df["close"].ewm(span=12, min_periods=12).mean()
        # Get the 12-day EMA of the closing price
        d = self.df["close"].ewm(span=26, min_periods=26).mean()
        # Subtract the 26-day EMA from the 12-Day EMA to get the MACD
        macd = k - d
        # Get the 9-Day EMA of the MACD for the Trigger line
        # Get the 9-Day EMA of the MACD for the Trigger line
        macd_s = macd.ewm(span=9, min_periods=9).mean()

        self.df["macd"] = macd
        self.df["macd_signal"] = macd_s

    def rsi(self, df):
        """
        Relative Strength Index (RSI) indicator
        https://www.qmr.ai/relative-strength-index-rsi-in-python/
        """

        change = df["close"].astype(float).diff()

        gain = change.mask(change < 0, 0.0)
        loss = -change.mask(change > 0, -0.0)

        # Verify that we did not make any mistakes
        change.equals(gain + loss)

        # Calculate the rolling average of average up and average down
        avg_up = gain.rolling(14).mean()
        avg_down = loss.rolling(14).mean().abs()

        rsi = 100 * avg_up / (avg_up + avg_down)
        df["rsi"] = rsi
        return df

    def ma_spreads(self):
        """
        Calculates spread based on bollinger bands,
        for later use in take profit and stop loss

        Returns:
        - top_band: diff between ma_25 and ma_100
        - bottom_band: diff between ma_7 and ma_25
        """

        band_1 = (abs(self.df["ma_100"] - self.df["ma_25"]) / self.df["ma_100"]) * 100
        band_2 = (abs(self.df["ma_25"] - self.df["ma_7"]) / self.df["ma_25"]) * 100

        self.df["big_ma_spread"] = band_1
        self.df["small_ma_spread"] = band_2

    def bollinguer_spreads(self, window=20, num_std=2):
        """
        Calculates Bollinguer bands

        https://www.kaggle.com/code/blakemarterella/pandas-bollinger-bands

        """
        bb_df = self.df.copy()
        bb_df["rolling_mean"] = bb_df["close"].rolling(window).mean()
        bb_df["rolling_std"] = bb_df["close"].rolling(window).std()
        bb_df["upper_band"] = bb_df["rolling_mean"] + (num_std * bb_df["rolling_std"])
        bb_df["lower_band"] = bb_df["rolling_mean"] - (num_std * bb_df["rolling_std"])

        self.df["bb_upper"] = bb_df["upper_band"]
        self.df["bb_lower"] = bb_df["lower_band"]
        self.df["bb_mid"] = bb_df["rolling_mean"]

        bb_df_1h = self.df_1h.copy()
        rolling_mean = bb_df_1h["close"].rolling(window).mean()
        bb_df_1h["rolling_std"] = bb_df_1h["close"].rolling(window).std()
        bb_df_1h["upper_band"] = rolling_mean + (num_std * bb_df_1h["rolling_std"])
        bb_df_1h["lower_band"] = rolling_mean - (num_std * bb_df_1h["rolling_std"])

        self.df_1h["bb_upper"] = bb_df_1h["upper_band"]
        self.df_1h["bb_lower"] = bb_df_1h["lower_band"]

        self.df_1h["bb_high"] = bb_df_1h["upper_band"]
        self.df_1h["bb_low"] = bb_df_1h["lower_band"]
        self.df_1h["bb_mid"] = rolling_mean

        return

    def log_volatility(self, window_size=7):
        """
        Volatility (standard deviation of returns) using logarithm, this normalizes data
        so it's easily comparable with other assets

        Returns:
        - Volatility in percentage
        """
        log_volatility = (
            Series(self.df["close"])
            .astype(float)
            .pct_change()
            .rolling(window_size)
            .std()
        )
        self.df["perc_volatility"] = log_volatility

    def set_twap(self, periods: int = 30) -> None:
        """
        Time-weighted average price
        https://stackoverflow.com/a/69517577/2454059

        Periods kept at 4 by default,
        otherwise there's not enough data
        """
        pre_df = self.df_1h.copy()
        pre_df["Event Time"] = pandas.to_datetime(pre_df["close_time"])
        pre_df["Time Diff"] = (
            pre_df["Event Time"].diff(periods=periods).dt.total_seconds() / 3600
        )
        pre_df["Weighted Value"] = pre_df["close"] * pre_df["Time Diff"]
        pre_df["Weighted Average"] = (
            pre_df["Weighted Value"].rolling(periods).sum() / pre_df["Time Diff"].sum()
        )
        # Fixed window of given interval
        self.df_1h["twap"] = pre_df["Weighted Average"]

        return

    def set_supertrend(self, df, period: int = 14, multiplier: float = 3.0) -> None:
        """
        Calculate the Supertrend indicator and add it to the DataFrame.
        """

        hl2 = (df["high"] + df["low"]) / 2

        # True Range (TR)
        previous_close = df["close"].shift(1)
        high_low = df["high"] - df["low"]
        high_pc = abs(df["high"] - previous_close)
        low_pc = abs(df["low"] - previous_close)
        tr = pandas.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)

        # Average True Range (ATR)
        df["atr"] = tr.rolling(window=period).mean()

        # Bands
        df["upperband"] = hl2 + (multiplier * df["atr"])
        df["lowerband"] = hl2 - (multiplier * df["atr"])

        supertrend = []

        for i in range(period, len(df)):
            if df["close"].iloc[i - 1] > df["upperband"].iloc[i - 1]:
                df.at[i, "upperband"] = max(
                    df["upperband"].iloc[i], df["upperband"].iloc[i - 1]
                )
            else:
                df.at[i, "upperband"] = df["upperband"].iloc[i]

            if df["close"].iloc[i - 1] < df["lowerband"].iloc[i - 1]:
                df.at[i, "lowerband"] = min(
                    df["lowerband"].iloc[i], df["lowerband"].iloc[i - 1]
                )
            else:
                df.at[i, "lowerband"] = df["lowerband"].iloc[i]

            # Determine trend direction
            if df["close"].iloc[i] > df["upperband"].iloc[i - 1]:
                supertrend.append(True)
            elif df["close"].iloc[i] < df["lowerband"].iloc[i - 1]:
                supertrend.append(False)

            df["supertrend"] = supertrend

            return

    async def publish(self):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        if self.df.empty is False and self.df.close.size > 0:
            # Basic technical indicators
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.moving_averages(7)
            self.moving_averages(25)
            self.moving_averages(100)

            # Oscillators
            self.macd()
            self.rsi(df=self.df)
            self.rsi(df=self.df_1h)

            # Bollinguer bands
            self.ma_spreads()
            self.bollinguer_spreads()

            self.log_volatility()
            self.set_twap()

            # Post-processing
            self.df.dropna(inplace=True)
            self.df.reset_index(drop=True, inplace=True)
            self.df_1h.dropna(inplace=True)
            self.df_1h.reset_index(drop=True, inplace=True)
            self.df_4h.dropna(inplace=True)
            self.df_4h.reset_index(drop=True, inplace=True)

            # Avoid repeated signals in short periods of time
            is_lapsed = self.symbol in self.repeated_signals and self.repeated_signals[
                self.symbol
            ] < datetime.now() - timedelta(minutes=30)

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df.ma_7.size < 7
                or self.df.ma_25.size < 25
                or self.df.ma_100.size < 100
                and not is_lapsed
            ):
                return

            if self.bot_strategy == Strategy.margin_short:
                return

            close_price = float(self.df.close[len(self.df.close) - 1])
            open_price = float(self.df.open[len(self.df.open) - 1])

            if self.btc_correlation == 0 or self.btc_price == 0:
                self.btc_correlation, self.btc_price = (
                    self.binbot_api.get_btc_correlation(symbol=self.symbol)
                )

            volatility = float(
                self.df.perc_volatility[len(self.df.perc_volatility) - 1]
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

            await top_gainers_drop(
                self,
                close_price=close_price,
                open_price=open_price,
                volatility=volatility,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )

            # avoid repeating signals in short periods of time
            self.repeated_signals[self.symbol] = datetime.now()

        return

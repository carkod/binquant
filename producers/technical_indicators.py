import logging
from datetime import datetime, timedelta

import pandas
import pandas_ta as ta
from aiokafka import AIOKafkaProducer
from algorithms.coinrule import (
    buy_low_sell_high,
    supertrend_swing_reversal,
    twap_momentum_sniper,
)
from algorithms.ma_candlestick import ma_candlestick_drop, ma_candlestick_jump
from algorithms.market_domination import MarketDominationAlgo
from algorithms.top_gainer_drop import top_gainers_drop
from shared.apis.binbot_api import BinbotApi
from shared.apis.time_gpt import TimeseriesGPT
from shared.enums import BinanceKlineIntervals, MarketDominance, Strategy
from shared.utils import round_numbers

class TechnicalIndicators:
    def __init__(
        self, base_producer, producer: AIOKafkaProducer, binbot_api: BinbotApi, df, symbol, df_4h, df_1h, market_domination_data, top_gainers_day
    ) -> None:
        """
        Only variables
        no data requests (third party or db)
        or pipeline instances
        That will cause a lot of network requests
        """
        self.base_producer = base_producer
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
        self.times_gpt_api = TimeseriesGPT()
        self.market_domination_data = market_domination_data
        self.top_gainers_day = top_gainers_day["data"]
        pass

    def check_kline_gaps(self, data):
        """
        Check data consistency

        Currently not implemented with kafka streams data
        as we need to check the nature of such gaps, as now
        data needs to be aggregated for larger windows > 1m
        i.e. 15m -> aggregate 1m * 15
        """

        ot = datetime.fromtimestamp(round(int(data["open_time"]) / 1000))
        ct = datetime.fromtimestamp(round(int(data["close_time"]) / 1000))
        time_diff: timedelta = ct - ot
        min_diff = int(time_diff.total_seconds() / 60)
        if self.interval == "15m":
            if min_diff > 15:
                logging.warning(f'Gap in {data["symbol"]} klines: {min_diff} minutes')

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

    def log_volatility(self, window_size=7):
        """
        Volatility (standard deviation of returns) using logarithm, this normalizes data
        so it's easily comparable with other assets

        Returns:
        - Volatility in percentage
        """
        log_volatility = (
            pandas.Series(self.df["close"])
            .astype(float)
            .pct_change()
            .rolling(window_size)
            .std()
        )
        self.df["perc_volatility"] = log_volatility

    def set_supertrend(self) -> None:
        """
        Supertrend indicator
        """
        st = ta.supertrend(
            self.df_1h["high"], self.df_1h["low"], self.df_1h["close"], 10, 3
        )
        self.df_1h["supertrend"] = st["SUPERT_10_3.0"]
        return

    def set_twap(self, periods: int = 12, interval=4) -> None:
        """
        Time-weighted average price
        https://stackoverflow.com/a/69517577/2454059
        """
        pre_df = self.df_4h.copy()
        pre_df["Event Time"] = pandas.to_datetime(pre_df["close_time"])
        pre_df["Time Diff"] = (
            pre_df["Event Time"].diff(periods=periods).dt.total_seconds() / 3600
        )
        pre_df["Weighted Value"] = pre_df["close"] * pre_df["Time Diff"]
        pre_df["Weighted Average"] = (
            pre_df["Weighted Value"].rolling(periods).sum() / pre_df["Time Diff"].sum()
        )
        # Fixed window of given interval
        self.df_4h["twap"] = pre_df["Weighted Average"]

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
            self.set_supertrend()
            self.set_twap()

            # Post-processing
            self.df.dropna(inplace=True)
            self.df.reset_index(drop=True, inplace=True)
            self.df_1h.dropna(inplace=True)
            self.df_1h.reset_index(drop=True, inplace=True)
            self.df_4h.dropna(inplace=True)
            self.df_4h.reset_index(drop=True, inplace=True)

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df.ma_7.size < 7
                or self.df.ma_25.size < 25
                or self.df.ma_100.size < 100
            ):
                return

            if self.bot_strategy == Strategy.margin_short:
                return

            close_price = float(self.df.close[len(self.df.close) - 1])
            open_price = float(self.df.open[len(self.df.open) - 1])
            rsi = float(self.df.rsi[len(self.df.rsi) - 1])
            ma_7 = float(self.df.ma_7[len(self.df.ma_7) - 1])
            ma_7_prev = float(self.df.ma_7[len(self.df.ma_7) - 2])
            ma_25 = float(self.df.ma_25[len(self.df.ma_25) - 1])
            ma_25_prev = float(self.df.ma_25[len(self.df.ma_25) - 2])
            ma_100 = float(self.df.ma_100[len(self.df.ma_100) - 1])
            # ma_100_prev = float(self.df.ma_100[len(self.df.ma_100) - 2])

            volatility = float(
                self.df.perc_volatility[len(self.df.perc_volatility) - 1]
            )

            bb_high, bb_mid, bb_low = self.bb_spreads()
            btc_correlation = self.binbot_api.get_btc_correlation(symbol=self.symbol)

            mda = MarketDominationAlgo(
                cls=self,
                close_price=close_price,
                bb_high=bb_high,
                bb_low=bb_low,
                bb_mid=bb_mid,
            )
            await mda.market_domination_signal(btc_correlation=btc_correlation)
            # Activate next month
            # mda.time_gpt_market_domination()

            # ma_candlestick_jump(
            #     self,
            #     close_price,
            #     open_price,
            #     ma_7,
            #     ma_25,
            #     ma_100,
            #     ma_7_prev,
            #     volatility,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            #     btc_correlation=btc_correlation,
            # )

            # ma_candlestick_drop(
            #     self,
            #     close_price=close_price,
            #     open_price=open_price,
            #     ma_7=ma_7,
            #     ma_100=ma_100,
            #     ma_25=ma_25,
            #     ma_25_prev=ma_25_prev,
            #     volatility=volatility,
            #     bb_high=bb_high,
            #     bb_mid=bb_mid,
            #     bb_low=bb_low,
            # )

            # buy_low_sell_high(
            #     self,
            #     close_price=close_price,
            #     rsi=rsi,
            #     ma_25=ma_25,
            #     volatility=volatility,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

            # This function calls a lot ticker24 revise it before uncommenting
            # rally_or_pullback(
            #     self,
            #     close_price=close_price,
            #     ma_25=ma_25,
            #     ma_100=ma_100,
            #     ma_25_prev=ma_25_prev,
            #     ma_100_prev=ma_100_prev,
            #     volatility=volatility,
            # )

            # top_gainers_drop(
            #     self,
            #     close_price=close_price,
            #     open_price=open_price,
            #     volatility=volatility,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

            # supertrend_swing_reversal(
            #     self,
            #     close_price=close_price,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

            # twap_momentum_sniper(
            #     self,
            #     close_price=close_price,
            #     bb_high=bb_high,
            #     bb_low=bb_low,
            #     bb_mid=bb_mid,
            # )

        return

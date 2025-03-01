import logging
from datetime import datetime, timedelta

import pandas

from algorithms.coinrule import buy_low_sell_high, fast_and_slow_macd
from algorithms.ma_candlestick import ma_candlestick_drop, ma_candlestick_jump
from algorithms.timeseries_gpt import TimeseriesGPT
from algorithms.top_gainer_drop import top_gainers_drop
from models.signals import BollinguerSpread, SignalsConsumer
from producers.base import BaseProducer
from shared.apis import BinbotApi
from shared.enums import BinanceKlineIntervals, KafkaTopics, MarketDominance, Strategy
from shared.utils import round_numbers


class TechnicalIndicators(BinbotApi):
    def __init__(self, df, symbol) -> None:
        self.base_producer = BaseProducer()
        self.base_producer.start_producer()
        self.producer = self.base_producer.producer
        self.df = df
        self.symbol = symbol
        self.interval = BinanceKlineIntervals.fifteen_minutes.value
        # describes current USDC market: gainers vs losers
        self.current_market_dominance: MarketDominance = MarketDominance.NEUTRAL
        # describes whether tide is shifting
        self.market_domination_reversal: bool = False
        self.bot_strategy: Strategy = Strategy.long
        self.active_pairs = self.get_active_pairs()
        self.top_coins_gainers: list[str] = []
        self.forecast = ""
        pass

    def update_active_bots_bb_spreads(self, close_price, symbol):
        """
        Update active bots with bb_spreads
        """
        bb_high, bb_mid, bb_low = self.bb_spreads()
        value = SignalsConsumer(
            spread=None,
            current_price=close_price,
            msg="Update active bots",
            symbol=symbol,
            algo="update_active_bots_bb_spreads",
            trend=None,
            bb_spreads=BollinguerSpread(bb_high=bb_high, bb_mid=bb_mid, bb_low=bb_low),
        )
        self.producer.send(
            KafkaTopics.signals.value, value=value.model_dump_json()
        ).add_callback(self.base_producer.on_send_success).add_errback(
            self.base_producer.on_send_error
        )

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

    def rsi(self):
        """
        Relative Strength Index (RSI) indicator
        https://www.qmr.ai/relative-strength-index-rsi-in-python/
        """

        change = self.df["close"].astype(float).diff()

        gain = change.mask(change < 0, 0.0)
        loss = -change.mask(change > 0, -0.0)

        # Verify that we did not make any mistakes
        change.equals(gain + loss)

        # Calculate the rolling average of average up and average down
        avg_up = gain.rolling(14).mean()
        avg_down = loss.rolling(14).mean().abs()

        rsi = 100 * avg_up / (avg_up + avg_down)
        self.df["rsi"] = rsi

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

    def time_gpt_forecast(self, data):
        """
        Forecasting using GPT-3
        """
        df = pandas.DataFrame(data)
        times_gpt = TimeseriesGPT(df)
        # df.rename(columns={'dates': 'ds', 'gainers_count': 'y'}, inplace=True)
        msf = times_gpt.multiple_series_forecast(df)
        return msf

    def market_domination(self) -> MarketDominance:
        """
        Get data from gainers and losers endpoint to analyze market trends

        We want to know when it's more suitable to do long positions
        when it's more suitable to do short positions
        For now setting threshold to 70% i.e.
        if > 70% of assets in a given market (USDT) dominated by gainers
        if < 70% of assets in a given market dominated by losers
        Establish the timing
        """
        if datetime.now().minute == 0:
            logging.info(
                f"Performing market domination analyses. Current trend: {self.current_market_dominance}"
            )
            data = self.get_market_domination_series()
            top_gainers_day = self.get_top_gainers()["data"]
            self.top_coins_gainers = [item["symbol"] for item in top_gainers_day]
            # reverse to make latest series more important
            data["gainers_count"].reverse()
            data["losers_count"].reverse()
            gainers_count = data["gainers_count"]
            losers_count = data["losers_count"]
            # no data from db
            if len(gainers_count) == 0 and len(losers_count) == 0:
                return self.current_market_dominance

            if len(data["dates"]) > 144:
                try:
                    self.forecast = self.time_gpt_forecast(data)
                except Exception as e:
                    logging.error(f"Error forecasting data: {e}")
                    pass

            # Proportion indicates whether trend is significant or not
            # to be replaced by TimesGPT if that works better
            proportion = max(gainers_count[-1], losers_count[-1]) / (
                gainers_count[-1] + losers_count[-1]
            )

            # Check reversal
            if gainers_count[-1] > losers_count[-1]:
                # Update current market dominance
                self.current_market_dominance = MarketDominance.GAINERS

                if (
                    gainers_count[-2] > losers_count[-2]
                    and gainers_count[-3] > losers_count[-3]
                    and proportion < 0.6
                ):
                    self.market_domination_reversal = True
                    self.bot_strategy = Strategy.long

            if gainers_count[-1] < losers_count[-1]:
                self.current_market_dominance = MarketDominance.LOSERS

                if (
                    gainers_count[-2] < losers_count[-2]
                    and (gainers_count[-3] < losers_count[-3])
                    and proportion < 0.6
                ):
                    # Negative reversal
                    self.market_domination_reversal = True
                    self.bot_strategy = Strategy.margin_short

        return self.current_market_dominance

    def publish(self):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        if self.df.empty is False and self.df.close.size > 0:
            # Bolliguer bands
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.moving_averages(7)
            self.moving_averages(25)
            self.moving_averages(100)

            # Oscillators
            self.macd()
            self.rsi()

            # Bollinguer bands
            self.ma_spreads()
            self.bollinguer_spreads()

            self.log_volatility()

            # Post-processing
            self.df.reset_index(drop=True, inplace=True)

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df.ma_7.size < 7
                or self.df.ma_25.size < 25
                or self.df.ma_100.size < 100
            ):
                return

            try:
                close_price = float(self.df.close[len(self.df.close) - 1])
                open_price = float(self.df.open[len(self.df.open) - 1])
                macd = float(self.df.macd[len(self.df.macd) - 1])
                macd_signal = float(self.df.macd_signal[len(self.df.macd_signal) - 1])
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

                if self.symbol in self.active_pairs:
                    self.update_active_bots_bb_spreads(
                        close_price=close_price, symbol=self.symbol
                    )
                    return

                self.market_domination()

                fast_and_slow_macd(
                    self,
                    close_price,
                    macd,
                    macd_signal,
                    ma_7,
                    ma_25,
                    volatility,
                )

                ma_candlestick_jump(
                    self,
                    close_price,
                    open_price,
                    ma_7,
                    ma_25,
                    ma_100,
                    ma_7_prev,
                    volatility,
                )

                ma_candlestick_drop(
                    self,
                    close_price=close_price,
                    open_price=open_price,
                    ma_7=ma_7,
                    ma_100=ma_100,
                    ma_25=ma_25,
                    ma_25_prev=ma_25_prev,
                    volatility=volatility,
                )

                buy_low_sell_high(self, close_price, rsi, ma_25, volatility)

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

                top_gainers_drop(
                    self,
                    close_price=close_price,
                    open_price=open_price,
                    volatility=volatility,
                )
            except Exception as e:
                logging.error(f"Error processing data: {e}")

        return

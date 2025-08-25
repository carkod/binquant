from pandas import DataFrame, Series, concat, to_datetime


class Indicators:
    """
    Technical indicators for financial data analysis
    this avoids using ta-lib because that requires
    dependencies that causes issues in the infrastructure
    """

    def moving_averages(df: DataFrame, period=7) -> DataFrame:
        """
        Calculate moving averages for 7, 25, 100 days
        this also takes care of Bollinguer bands
        """
        df[f"ma_{period}"] = df["close"].rolling(window=period).mean()
        return df

    def macd(df: DataFrame) -> DataFrame:
        """
        Moving Average Convergence Divergence (MACD) indicator
        https://www.alpharithms.com/calculate-macd-python-272222/
        """

        k = df["close"].ewm(span=12, min_periods=12).mean()
        # Get the 12-day EMA of the closing price
        d = df["close"].ewm(span=26, min_periods=26).mean()
        # Subtract the 26-day EMA from the 12-Day EMA to get the MACD
        macd = k - d
        # Get the 9-Day EMA of the MACD for the Trigger line
        # Get the 9-Day EMA of the MACD for the Trigger line
        macd_s = macd.ewm(span=9, min_periods=9).mean()

        df["macd"] = macd
        df["macd_signal"] = macd_s

        return df

    def rsi(df: DataFrame) -> DataFrame:
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

    def ma_spreads(df: DataFrame) -> DataFrame:
        """
        Calculates spread based on bollinger bands,
        for later use in take profit and stop loss

        Returns:
        - top_band: diff between ma_25 and ma_100
        - bottom_band: diff between ma_7 and ma_25
        """

        band_1 = (abs(df["ma_100"] - df["ma_25"]) / df["ma_100"]) * 100
        band_2 = (abs(df["ma_25"] - df["ma_7"]) / df["ma_25"]) * 100

        df["big_ma_spread"] = band_1
        df["small_ma_spread"] = band_2

        return df

    def bollinguer_spreads(df: DataFrame, window=20, num_std=2) -> DataFrame:
        """
        Calculates Bollinguer bands

        https://www.kaggle.com/code/blakemarterella/pandas-bollinger-bands

        """
        bb_df = df.copy()
        bb_df["rolling_mean"] = bb_df["close"].rolling(window).mean()
        bb_df["rolling_std"] = bb_df["close"].rolling(window).std()
        bb_df["upper_band"] = bb_df["rolling_mean"] + (num_std * bb_df["rolling_std"])
        bb_df["lower_band"] = bb_df["rolling_mean"] - (num_std * bb_df["rolling_std"])

        df["bb_upper"] = bb_df["upper_band"]
        df["bb_lower"] = bb_df["lower_band"]
        df["bb_mid"] = bb_df["rolling_mean"]

        return df

    def log_volatility(df: DataFrame, window_size=7) -> DataFrame:
        """
        Volatility (standard deviation of returns) using logarithm, this normalizes data
        so it's easily comparable with other assets

        Returns:
        - Volatility in percentage
        """
        log_volatility = (
            Series(df["close"]).astype(float).pct_change().rolling(window_size).std()
        )
        df["perc_volatility"] = log_volatility

        return df

    def set_twap(df: DataFrame, periods: int = 30) -> DataFrame:
        """
        Time-weighted average price
        https://stackoverflow.com/a/69517577/2454059

        Periods kept at 4 by default,
        otherwise there's not enough data
        """
        pre_df = df.copy()
        pre_df["Event Time"] = to_datetime(pre_df["close_time"])
        pre_df["Time Diff"] = (
            pre_df["Event Time"].diff(periods=periods).dt.total_seconds() / 3600
        )
        pre_df["Weighted Value"] = pre_df["close"] * pre_df["Time Diff"]
        pre_df["Weighted Average"] = (
            pre_df["Weighted Value"].rolling(periods).sum() / pre_df["Time Diff"].sum()
        )
        # Fixed window of given interval
        df["twap"] = pre_df["Weighted Average"]

        return df

    def set_supertrend(
        df: DataFrame, period: int = 14, multiplier: float = 3.0
    ) -> DataFrame:
        """
        Calculate the Supertrend indicator and add it to the DataFrame.
        """

        hl2 = (df["high"] + df["low"]) / 2

        # True Range (TR)
        previous_close = df["close"].shift(1)
        high_low = df["high"] - df["low"]
        high_pc = abs(df["high"] - previous_close)
        low_pc = abs(df["low"] - previous_close)
        tr = concat([high_low, high_pc, low_pc], axis=1).max(axis=1)

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

        return df

from pandas import DataFrame, Series, concat, to_datetime


class Indicators:
    """
    Technical indicators for financial data analysis
    this avoids using ta-lib because that requires
    dependencies that causes issues in the infrastructure
    """

    original_cols = [
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
    binance_cols = [
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
    kucoin_cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
    ]

    def post_process(df: DataFrame) -> DataFrame:
        """
        Post-process the DataFrame by filling missing values and
        converting data types as needed.
        """
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

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

    def rsi(df: DataFrame, window: int = 14) -> DataFrame:
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
        avg_up = gain.rolling(window).mean()
        avg_down = loss.rolling(window).mean().abs()

        rsi = 100 * avg_up / (avg_up + avg_down)
        df["rsi"] = rsi

        return df

    def standard_rsi(df: DataFrame, window: int = 14) -> DataFrame:
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

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
        """Compute Supertrend indicator.

        Adds columns: 'atr', 'upperband', 'lowerband', 'supertrend'.
        'supertrend' is a boolean (or None before initialization) indicating
        bullish (True) or bearish (False) trend.
        """
        if len(df) == 0:
            return df

        hl2 = (df["high"] + df["low"]) / 2.0
        prev_close = df["close"].shift(1)
        tr = concat(
            [
                (df["high"] - df["low"]),
                (df["high"] - prev_close).abs(),
                (df["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)

        df["atr"] = tr.rolling(window=period).mean()
        upperband = hl2 + multiplier * df["atr"]
        lowerband = hl2 - multiplier * df["atr"]

        # Smooth bands
        for i in range(period, len(df)):
            if df["close"].iloc[i - 1] > upperband.iloc[i - 1]:
                upperband.iloc[i] = max(upperband.iloc[i], upperband.iloc[i - 1])
            if df["close"].iloc[i - 1] < lowerband.iloc[i - 1]:
                lowerband.iloc[i] = min(lowerband.iloc[i], lowerband.iloc[i - 1])

        direction: list[bool | None] = [None] * len(df)
        for i in range(period, len(df)):
            if df["close"].iloc[i] > upperband.iloc[i - 1]:
                direction[i] = True
            elif df["close"].iloc[i] < lowerband.iloc[i - 1]:
                direction[i] = False
            else:
                direction[i] = direction[i - 1]

        df["upperband"] = upperband
        df["lowerband"] = lowerband
        df["supertrend"] = direction
        return df

    def atr(df: DataFrame, window: int = 30, min_periods: int = 20) -> DataFrame:
        df["open_time"] = to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time", inplace=True)
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)

        # ATR breakout logic
        tr = concat(
            [
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)

        df["amplitude"] = df["high"] - df["low"]

        df["ATR"] = tr.rolling(window=window, min_periods=min_periods).mean()
        df["rolling_high"] = df["high"].rolling(window=window).max().shift(1)
        df["breakout_strength"] = (df["close"] - df["rolling_high"]) / df["ATR"]
        df["ATR_breakout"] = (df["close"] > (df["rolling_high"] + 1.1 * df["ATR"])) & (
            df["breakout_strength"] > 0.05
        )
        return df

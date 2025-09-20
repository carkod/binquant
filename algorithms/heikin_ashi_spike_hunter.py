import logging
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from algorithms.heikin_ashi import HeikinAshi
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import Strategy
from shared.indicators import Indicators

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class HASpikeHunter(HeikinAshi):
    """
    Unified spike detection system for cryptocurrency assets.
    Features:
    - Multiple detection algorithms (price+volume, momentum, RSI reversal, ML classifier)
    - Configurable sensitivity levels
    - Robust error handling
    - Performance optimized
    - Detailed summary and last spike reporting
    - Dynamic thresholding and advanced stats
    - ML-based spike prediction (RandomForest)
    """

    def __init__(
        self,
        cls: "CryptoAnalytics",
    ):
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model_v1.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = joblib.load(abs_file_path)

        self.momentum_threshold = 0.015
        self.window = 12
        self.rsi_oversold = 30
        # thresholds have to match trained model (binbot-notebooks)
        # otherwise they may overfit/underfit
        self.price_threshold = 0.02
        self.volume_threshold = 2

        # dependencies
        self.ti = cls
        self.df = self.get_heikin_ashi(cls.df)
        self.current_market_dominance = cls.current_market_dominance

    def match_loser(self, symbol: str):
        """
        Match the symbol with the top losers of the day.
        """
        for loser in self.ti.top_losers_day:
            if loser["symbol"] == symbol:
                return True
        return False

    def fetch_adr_series(self, size=500):
        data = self.ti.binbot_api.get_adr_series(size=size)
        df_adr = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(data["timestamp"]),
                "adp_ma": data["adp_ma"],
                "advancers": data["advancers"],
                "decliners": data["decliners"],
                "adp": data["adp"],
                "total_volume": data["total_volume"],
            }
        )
        return df_adr

    def calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def add_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # Basic price features
        df["price_change"] = df["close"].pct_change()
        df["price_change_abs"] = df["price_change"].abs()
        df["body_size"] = abs(df["close"] - df["open"])
        df["body_size_pct"] = df["body_size"] / df["open"]
        df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
        df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
        df["upper_wick_ratio"] = df["upper_wick"] / (df["body_size"] + 1e-6)
        df["lower_wick_ratio"] = df["lower_wick"] / (df["body_size"] + 1e-6)
        df["total_range"] = df["high"] - df["low"]
        df["range_pct"] = df["total_range"] / df["open"]
        df["is_bullish"] = (df["close"] > df["open"]).astype(int)
        df["close_open_ratio"] = (df["close"] - df["open"]) / (df["open"] + 1e-6)
        # Rolling features
        effective_window = min(self.window, len(df) // 4)
        if effective_window < 3:
            effective_window = 3
        df["price_ma"] = df["close"].rolling(window=effective_window).mean()
        df["price_std"] = df["close"].rolling(window=effective_window).std()
        df["price_zscore"] = (df["close"] - df["price_ma"]) / (df["price_std"] + 1e-6)
        # Volume features
        df["volume_ma"] = df["volume"].rolling(window=effective_window).mean()
        df["volume_ratio"] = df["volume"] / (df["volume_ma"] + 1e-6)
        df["volume_zscore"] = (df["volume"] - df["volume_ma"]) / (
            df["volume"].rolling(window=effective_window).std() + 1e-6
        )
        # Momentum indicators
        df["momentum_3"] = df["close"].pct_change(3)
        df["momentum_5"] = df["close"].pct_change(5)
        # High/Low features
        df["high_low_ratio"] = df["high"] / df["low"]
        df["close_to_high"] = (df["high"] - df["close"]) / (df["high"] + 1e-6)
        df["close_to_low"] = (df["close"] - df["low"]) / (df["close"] + 1e-6)
        # RSI
        df["rsi"] = self.calculate_rsi(df["close"])
        return df

    def get_feature_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        feature_cols = [
            "price_change",
            "price_change_abs",
            "body_size_pct",
            "upper_wick_ratio",
            "lower_wick_ratio",
            "is_bullish",
            "close_open_ratio",
            "price_zscore",
            "volume_ratio",
            "volume_zscore",
            "momentum_3",
            "momentum_5",
            "range_pct",
            "close_to_high",
            "close_to_low",
        ]
        for col in feature_cols:
            if col not in df.columns:
                df[col] = 0.0
        return df[feature_cols].fillna(0)

    def detect_spikes(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.add_all_features(df)
        df["spike_signal"] = 0
        df["spike_type"] = ""
        df["signal_strength"] = 0.0

        # ML-based spike prediction
        X = self.get_feature_matrix(df)
        try:
            ml_preds = self.model.predict(X)
        except Exception:
            ml_preds = np.zeros(len(df))

        effective_window = min(self.window, len(df) // 4)
        if effective_window < 3:
            effective_window = 3

        # Use fixed thresholds if set
        # otherwise model can overfit
        price_thresh = (
            self.price_threshold
            if self.price_threshold is not None
            else df["price_change"].quantile(0.95)
        )
        volume_thresh = (
            self.volume_threshold
            if self.volume_threshold is not None
            else df["volume_ratio"].quantile(0.90)
        )

        for i in range(effective_window, len(df)):
            signal = 0
            signal_type = ""
            strength = 0.0
            current_price_change = df.iloc[i]["price_change"]
            current_volume_ratio = df.iloc[i]["volume_ratio"]
            current_rsi = df.iloc[i]["rsi"]
            # Method 1: ML classifier
            if ml_preds[i] == 1:
                signal = 1
                signal_type = "ml_classifier"
                strength = 10.0
            # Method 2: Price + Volume Spike
            elif (
                current_price_change > price_thresh
                and current_volume_ratio > volume_thresh
            ):
                signal = 1
                signal_type = "price_volume"
                strength = min(current_price_change * current_volume_ratio * 10, 10.0)
            # Method 3: Momentum Detection
            elif i >= 5:
                recent_changes = df.iloc[i - 3 : i]["price_change"]
                positive_moves = (recent_changes > self.momentum_threshold).sum()
                total_momentum = recent_changes.sum()
                if positive_moves >= 2 and total_momentum > price_thresh:
                    signal = 1
                    signal_type = "momentum"
                    strength = min(total_momentum * 20, 8.0)
            # Method 4: RSI Oversold Reversal
            elif (
                i >= 14
                and current_rsi > self.rsi_oversold
                and df.iloc[i - 1]["rsi"] <= self.rsi_oversold
                and current_price_change > 0.008
            ):
                signal = 1
                signal_type = "rsi_reversal"
                strength = min(
                    (current_rsi - self.rsi_oversold) / 10 * current_price_change * 50,
                    6.0,
                )
            df.at[i, "spike_signal"] = signal
            df.at[i, "spike_type"] = signal_type
            df.at[i, "signal_strength"] = strength
        return df

    def run_analysis(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        # If fixed thresholds are not set, set them dynamically here (for backward compatibility)
        if self.price_threshold is None:
            self.price_threshold = df["price_change"].quantile(0.95)
        if self.volume_threshold is None:
            self.volume_threshold = df["volume_ratio"].quantile(0.90)
        df = self.detect_spikes(df)
        return df

    def get_last_spike_details(self, df, max_minutes_ago=30):
        df = Indicators.post_process(df)
        spikes = df[df["spike_signal"] == 1]
        if len(spikes) == 0:
            return None
        last_spike = spikes.iloc[-1]
        current_time = pd.Timestamp.now(tz="UTC")
        spike_time = last_spike["timestamp"]
        if spike_time.tz is None:
            spike_time = spike_time.tz_localize("UTC")
        minutes_ago = (current_time - spike_time).total_seconds() / 60
        if minutes_ago > max_minutes_ago:
            return None

        return {
            "timestamp": last_spike["timestamp"],
            "price": last_spike["close"],
            "price_change": last_spike["price_change"],
            "price_change_pct": last_spike["price_change"] * 100,
            "volume": last_spike["volume"],
            "volume_ratio": last_spike["volume_ratio"],
            "spike_type": last_spike["spike_type"],
            "signal_strength": last_spike["signal_strength"],
            "rsi": last_spike["rsi"],
            "body_size_pct": last_spike["body_size_pct"],
            "minutes_ago": minutes_ago,
        }

    async def get_spikes(self):
        """
        Generate a signal based on the spike prediction.
        """
        # Use the current DataFrame from technical indicators
        current_df = self.ti.df.copy()

        fresh_df = self.run_analysis(current_df)

        if fresh_df is None:
            return None

        # Check for spikes in different time windows
        time_windows = [5, 15, 30, 60]  # 5 minutes, 15 minutes

        for window in time_windows:
            last_spike = self.get_last_spike_details(fresh_df, max_minutes_ago=window)

            if last_spike:
                break

        return last_spike

    async def ha_spike_hunter_standard(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        """
        Standard spike hunter algorithm that detects spikes with no confirmations.
        """
        last_spike = await self.get_spikes()

        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )

        if not last_spike:
            logging.debug("No recent spike detected for breakout.")
            return

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if current_price > bb_high:
            algo = "ha_spike_hunter_standard"
            autotrade = False

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - 📅 Time: {last_spike["timestamp"].strftime("%Y-%m-%d %H:%M")}
                - 📈 Price: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - ⚡ Strength: {last_spike["signal_strength"] / 10:.1f}
                - BTC Correlation: {self.ti.btc_correlation:.2f}
                - Autotrade?: {"Yes" if autotrade else "No"}
                - ADP diff: {adp_diff:.2f} (prev: {adp_diff_prev:.2f})
                - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
                - <a href='http://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
                """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=current_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=Strategy.long,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

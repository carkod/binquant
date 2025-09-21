import logging
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import MarketDominance, Strategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class SpikeHunter:
    """
    Unified spike detection system for cryptocurrency assets.
    Features:
    - Multiple detection algorithms (price+volume, momentum, RSI reversal, ML classifier)
    - Per-type boolean columns (spike_ml_classifier, spike_price_volume, spike_momentum, spike_rsi_reversal)
    - Removed legacy single spike_type / spike_types list in favor of explicit columns
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

        self.momentum_threshold = 0.012
        self.window = 12
        self.rsi_oversold = 30
        # thresholds have to match trained model (binbot-notebooks)
        # otherwise they may overfit/underfit
        self.price_threshold = 0.025
        self.volume_threshold = 2.5

        # dependencies
        self.ti = cls
        self.df = cls.clean_df.copy()
        self.current_market_dominance = cls.current_market_dominance

        # Check for spikes in different time windows in mins
        # because of delays in kafka streaming, this could be up to 2 hours
        self.time_windows = [15, 30, 60, 120, 240, 300]
        self.mechanism_cols = [
            ("ml_classifier", "spike_ml_classifier"),
            ("price_volume", "spike_price_volume"),
            ("momentum", "spike_momentum"),
            ("rsi_reversal", "spike_rsi_reversal"),
        ]

    def match_loser(self, symbol: str):
        """
        Match the symbol with the top losers of the day.
        """
        for loser in self.ti.top_losers_day:
            if loser["symbol"] == symbol:
                return True
        return False

    def calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def add_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df["timestamp"] = pd.to_datetime(df["close_time"], unit="ms")
        df = df.sort_values(by="timestamp").reset_index(drop=True)
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
        df = df.copy()
        df = df.reset_index(drop=True)
        df = self.add_all_features(df)
        # Initialize unified signal and strength plus per-mechanism flags
        df["spike_signal"] = 0
        df["signal_strength"] = 0.0
        # Per-type boolean indicator columns
        df["spike_ml_classifier"] = 0
        df["spike_price_volume"] = 0
        df["spike_momentum"] = 0
        df["spike_rsi_reversal"] = 0
        # Each row may have multiple mechanisms active. spike_signal=1 if any mechanism fires.
        # signal_strength is max of contributing mechanism strengths.

        # ML-based spike prediction
        X = self.get_feature_matrix(df)
        try:
            ml_preds = self.model.predict(X)
        except Exception:
            ml_preds = np.zeros(len(df))

        effective_window = min(self.window, len(df) // 4)
        if effective_window < 3:
            effective_window = 3

        # Use fixed thresholds if set, otherwise fallback to dynamic (will be set in run_analysis)
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
            current_price_change = df.loc[i, "price_change"]
            current_volume_ratio = df.loc[i, "volume_ratio"]
            current_rsi = df.loc[i, "rsi"]

            strengths = []

            # Method 1: ML classifier
            if ml_preds[i] == 1:
                df.loc[i, "spike_ml_classifier"] = 1
                strengths.append(10.0)

            # Method 2: Price + Volume Spike
            if (
                current_price_change > price_thresh
                and current_volume_ratio > volume_thresh
            ):
                df.loc[i, "spike_price_volume"] = 1
                strengths.append(
                    min(current_price_change * current_volume_ratio * 10, 10.0)
                )

            # Method 3: Momentum Detection
            if i >= 5:
                recent_changes = df.loc[i - 3 : i, "price_change"]
                positive_moves = (recent_changes > self.momentum_threshold).sum()
                total_momentum = recent_changes.sum()
                if positive_moves >= 2 and total_momentum > price_thresh:
                    df.loc[i, "spike_momentum"] = 1
                    strengths.append(min(total_momentum * 20, 8.0))

            # Method 4: RSI Oversold Reversal
            if (
                i >= 14
                and current_rsi > self.rsi_oversold
                and df.loc[i - 1, "rsi"] <= self.rsi_oversold
                and current_price_change > 0.008
            ):
                rsi_strength = min(
                    (current_rsi - self.rsi_oversold) / 10 * current_price_change * 50,
                    6.0,
                )
                df.loc[i, "spike_rsi_reversal"] = 1
                strengths.append(rsi_strength)

            if strengths:
                df.loc[i, "spike_signal"] = 1
                df.loc[i, "signal_strength"] = max(strengths)
        return df

    def run_analysis(self, max_minutes_ago=30):
        if self.price_threshold is None:
            self.price_threshold = self.df["price_change"].quantile(0.95)
        if self.volume_threshold is None:
            self.volume_threshold = self.df["volume_ratio"].quantile(0.90)

        self.df = self.detect_spikes(self.df)
        self.df.reset_index(drop=True, inplace=True)
        spikes = self.df[self.df["spike_signal"] == 1]
        if len(spikes) == 0:
            return None
        last_spike = spikes.iloc[-1]
        # Derive composite spike_type string from active mechanism columns

        active_types = [
            name for name, col in self.mechanism_cols if last_spike.get(col, 0) == 1
        ]
        spike_type_str = ",".join(active_types)
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
            "spike_type": spike_type_str,
            "signal_strength": last_spike["signal_strength"],
            "rsi": last_spike["rsi"],
            "body_size_pct": last_spike["body_size_pct"],
            "minutes_ago": minutes_ago,
        }

    def get_spikes(self):
        """
        Generate a signal based on the spike prediction.
        """
        last_spike = None
        for window in self.time_windows:
            last_spike = self.run_analysis(max_minutes_ago=window)
            if last_spike:
                break

        return last_spike

    async def spike_hunter_bullish(self, current_price, bb_high, bb_low, bb_mid):
        """
        Detect bullish spikes and send signals.
        """

        last_spike = self.get_spikes()

        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )

        if (
            last_spike
            and self.current_market_dominance == MarketDominance.LOSERS
            and adp_diff > 0
            and adp_diff_prev > 0
        ):
            algo = "spike_hunter_bullish"
            autotrade = True

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - 💪 Strength: {last_spike["signal_strength"]:.1f}
                - ₿ Correlation: {self.ti.btc_correlation:.2f}
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
            return True

    async def spike_hunter_breakouts(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        last_spike = self.get_spikes()

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
            algo = "spike_hunter_breakout"
            autotrade = True

            if self.match_loser(self.ti.symbol):
                algo = "spike_hunter_top_loser"
                autotrade = False

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - 💪 Strength: {last_spike["signal_strength"]:.1f}
                - ₿ Correlation: {self.ti.btc_correlation:.2f}
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

            return True

    async def spike_hunter_standard(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        """
        Standard spike hunter algorithm that detects spikes with no confirmations.
        """
        last_spike = self.get_spikes()

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
        if (
            last_spike
            and self.ti.btc_correlation < 0
            and current_price > bb_high
            and self.ti.btc_price < 0
        ):
            algo = "spike_hunter_standard"
            autotrade = True

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - 💪 Strength: {last_spike["signal_strength"]:.1f}
                - ₿ Correlation: {self.ti.btc_correlation:.2f}
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

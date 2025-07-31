import os
from os import path
from typing import TYPE_CHECKING

import joblib
import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class SpikeHunter:
    def __init__(self, cls: "TechnicalIndicators"):
        """
        Initialize the Isolation Forest model with a specified contamination level.

        :param contamination: The proportion of outliers in the data.
        """
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = joblib.load(abs_file_path)
        self.binbot = BinbotApi()
        self.ti = cls
        self.current_market_dominance = cls.current_market_dominance
        self.feature_cols = [
            "price_change",
            "body_size",
            "upper_wick_ratio",
            "lower_wick_ratio",
            "is_bullish",
            "close_open_ratio",
            "adp_ma",
            "adp",
            "advancers",
            "decliners",
        ]

    def match_loser(self, symbol: str):
        """
        Match the symbol with the top losers of the day.
        """
        for loser in self.ti.top_losers_day:
            if loser["symbol"] == symbol:
                return True
        return False

    def fetch_adr_series(self, size=500):
        data = self.binbot.get_adr_series(size=size)
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
        """Calculate RSI indicator efficiently."""
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / (loss + 1e-10)  # Avoid division by zero
        return 100 - (100 / (1 + rs))

    def get_last_spike_details(self, df: pd.DataFrame, max_minutes_ago=30):
        """
        Extract detailed information about the most recent spike detection.

        Args:
            df: DataFrame with spike analysis results
            max_minutes_ago: Maximum minutes ago to consider a spike valid
        Returns:
            Dictionary with last spike details or None if no spikes found
        """
        spikes = df[df["spike_signal"] == 1]

        if len(spikes) == 0:
            return None

        # Get the most recent spike (last chronologically)
        last_spike = spikes.iloc[-1]

        # Calculate how many minutes ago this spike occurred
        current_time = pd.Timestamp.now(tz="UTC")
        spike_time = last_spike["timestamp"]

        # Handle timezone-naive timestamps by assuming UTC
        if spike_time.tz is None:
            spike_time = spike_time.tz_localize("UTC")

        minutes_ago = (current_time - spike_time).total_seconds() / 60

        # Only return spike if it's within the time window
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

    def detect_spikes(
        self,
        df: pd.DataFrame,
        window: int = 12,
        price_threshold: float = 0.01,
        volume_threshold: float = 2.0,
        momentum_threshold: float = 0.02,
        rsi_oversold: float = 30.0,
    ) -> pd.DataFrame:
        """Main spike detection algorithm."""
        df = df.copy()

        # pre-processing
        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df.sort_values("timestamp").reset_index(drop=True)

        # Basic calculations
        df["price_change"] = df["close"].pct_change()
        df["body_size"] = abs(df["close"] - df["open"])
        df["body_size_pct"] = df["body_size"] / df["open"]

        # Volume analysis with adaptive window
        effective_window = min(window, len(df) // 4)
        if effective_window < 3:
            effective_window = 3

        df["volume_ma"] = (
            df["volume"].rolling(window=effective_window, min_periods=1).mean()
        )
        df["volume_ratio"] = df["volume"] / (df["volume_ma"] + 1e-10)

        # RSI calculation
        df["rsi"] = self.calculate_rsi(df["close"])

        # Initialize spike signals
        df["spike_signal"] = 0
        df["spike_type"] = ""
        df["signal_strength"] = 0.0

        # Main detection loop
        for i in range(effective_window, len(df)):
            signal = 0
            signal_type = ""
            strength = 0.0

            current_price_change = df.loc[i, "price_change"]
            current_volume_ratio = df.loc[i, "volume_ratio"]
            current_rsi = df.loc[i, "rsi"]

            # Method 1: Price + Volume Spike
            if (
                current_price_change > price_threshold
                and current_volume_ratio > volume_threshold
            ):
                signal = 1
                signal_type = "price_volume"
                strength = min(current_price_change * current_volume_ratio * 10, 10.0)

            # Method 2: Momentum Detection
            elif i >= 5:
                recent_changes = df.loc[i - 3 : i, "price_change"]
                positive_moves = (recent_changes > momentum_threshold).sum()
                total_momentum = recent_changes.sum()

                if positive_moves >= 2 and total_momentum > price_threshold:
                    signal = 1
                    signal_type = "momentum"
                    strength = min(total_momentum * 20, 8.0)

            # Method 3: RSI Oversold Reversal
            elif (
                i >= 14
                and current_rsi > rsi_oversold
                and df.loc[i - 1, "rsi"] <= rsi_oversold
                and current_price_change > 0.008
            ):
                signal = 1
                signal_type = "rsi_reversal"
                strength = min(
                    (current_rsi - rsi_oversold) / 10 * current_price_change * 50,
                    6.0,
                )

            df.loc[i, "spike_signal"] = signal
            df.loc[i, "spike_type"] = signal_type
            df.loc[i, "signal_strength"] = strength

        return df

    def run_analysis(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Run complete spike analysis for a trading pair."""
        # Detect spikes
        df = self.detect_spikes(df)

        # Generate summary
        spikes = df[df["spike_signal"] == 1]
        summary = {
            "total_spikes": len(spikes),
            "spike_rate": len(spikes) / len(df) if len(df) > 0 else 0,
            "date_range": f"{df['timestamp'].min()} to {df['timestamp'].max()}",
        }

        return df, summary

    async def signal(
        self,
        df: pd.DataFrame,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        """
        Generate a signal based on the spike prediction.
        """
        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )
        algo = "spike_hunter"
        autotrade = False

        if self.match_loser(self.ti.symbol):
            algo = "top_loser_spike_hunter"

        fresh_df, _ = self.run_analysis(df)

        # Check for spikes in different time windows
        time_windows = [5, 15, 30]  # 5 minutes, 15 minutes, 30 minutes

        spike_found = False
        for window in time_windows:
            last_spike = self.get_last_spike_details(fresh_df, max_minutes_ago=window)

            if last_spike:
                spike_found = True
                break

        if spike_found:
            algo = "spike_hunter"

            if (
                self.current_market_dominance == MarketDominance.LOSERS
                and adp_diff > 0
                and adp_diff_prev > 0
            ):
                algo += "_bullish"
                autotrade = True

            # When no bullish conditions, check for breakout spikes
            # btc correlation avoids tightly coupled assets
            if self.ti.btc_correlation < 0 and current_price > bb_high:
                algo += "_breakout"
                autotrade = False

            if self.ti.symbol in self.ti.top_losers_day:
                algo += "_top_loser"
                autotrade = True

            msg = f"""
            - 🔥 [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - 📅 Time: {last_spike["timestamp"].strftime("%Y-%m-%d %H:%M")}
            - 📈 Price: +{last_spike["price_change_pct"]}
            - 📊 Volume: {last_spike["volume_ratio"]}x above average
            - 🏷️ Type: {last_spike["spike_type"]}
            - ⚡ Strength: {last_spike["signal_strength"] / 10:.1f}
            - 📉 RSI: {last_spike["rsi"]:.1f}
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
            await self.ti.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

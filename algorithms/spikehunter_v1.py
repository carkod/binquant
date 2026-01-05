import logging
from datetime import UTC, datetime
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd
from pybinbot import MarketDominance, Strategy, HABollinguerSpread, SignalsConsumer
from shared.heikin_ashi import HeikinAshi
from shared.utils import safe_format

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
        df = cls.df.copy()
        self.df = HeikinAshi.get_heikin_ashi(df)
        self.current_market_dominance = cls.current_market_dominance

        # Check for spikes in different time windows in mins
        # because of delays in kafka streaming, this could be up to 2 hours
        self.time_windows = [15, 30, 60, 120]
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

    def cleanup(self):
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    def add_all_features(self) -> pd.DataFrame:
        df = self.df
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

        # Quote asset volume features
        df["quote_volume_ma"] = (
            df["quote_asset_volume"].rolling(window=effective_window).mean()
        )
        # Vectorized ratio; avoid calling float() on entire Series (raises TypeError)
        # Add small epsilon to denominator to avoid division by zero; result will be NaN
        # for initial rows where rolling mean is NaN and later cleaned up by self.cleanup().
        df["quote_volume_ratio"] = df["quote_asset_volume"] / (
            df["quote_volume_ma"] + 1e-6
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
        self.df = df
        return self.df

    def get_feature_matrix(self) -> pd.DataFrame:
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
            "taker_base",
            "taker_quote",
            "taker_buy_ratio",
            "quote_asset_volume",
            "quote_volume_ratio",
        ]
        for col in feature_cols:
            if col not in self.df.columns:
                self.df[col] = 0.0
        return self.df[feature_cols].fillna(0)

    def detect_spikes(self) -> pd.DataFrame:
        df = self.df
        self.cleanup()
        df = self.add_all_features()
        self.cleanup()
        df["spike_ml_classifier"] = 0
        df["spike_price_volume"] = 0
        df["spike_momentum"] = 0
        df["spike_rsi_reversal"] = 0
        df["spike_signal"] = 0

        # ML-based spike prediction
        X = self.get_feature_matrix()
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
            is_flat_candle = (
                float(df.loc[i, "quote_asset_volume"]) > 0
                and df.loc[i, "number_of_trades"] > 5
            )
            quote_volume_ratio = (
                df.loc[i, "quote_volume_ratio"]
                if "quote_volume_ratio" in df.columns
                else 0.0
            )
            liquidity_multiplier = 1 + min(quote_volume_ratio, 5) * 0.05

            if is_flat_candle and df.loc[i, "close"] == df.loc[i, "open"]:
                return self.df

            # Method 1: ML classifier
            if ml_preds[i] == 1 and is_flat_candle:
                df.loc[i, "spike_ml_classifier"] = 1
                df.loc[i, "spike_signal"] += 1

            # Method 2: Price + Volume Spike
            if (
                current_price_change > price_thresh
                and liquidity_multiplier * current_volume_ratio > volume_thresh
            ):
                df.loc[i, "spike_price_volume"] = 1
                df.loc[i, "spike_signal"] += 1

            # Method 3: Momentum Detection
            if i >= 5:
                recent_changes = df.loc[i - 3 : i, "price_change"]
                positive_moves = (recent_changes > self.momentum_threshold).sum()
                total_momentum = recent_changes.sum()
                if positive_moves >= 2 and total_momentum > price_thresh:
                    df.loc[i, "spike_momentum"] = 1
                    df.loc[i, "spike_signal"] += 1

            # Method 4: RSI Oversold Reversal
            if (
                i >= 14
                and current_rsi > self.rsi_oversold
                and df.loc[i - 1, "rsi"] <= self.rsi_oversold
                and current_price_change > 0.008
            ):
                df.loc[i, "spike_rsi_reversal"] = 1
                df.loc[i, "spike_signal"] += 1

        self.df = df
        return self.df

    def run_analysis(self, max_minutes_ago=30):
        if self.price_threshold is None:
            self.price_threshold = self.df["price_change"].quantile(0.95)
        if self.volume_threshold is None:
            self.volume_threshold = self.df["volume_ratio"].quantile(0.90)

        self.df = self.detect_spikes()
        self.df.dropna(inplace=True)
        spikes = self.df[self.df["spike_signal"] > 0]
        if len(spikes) == 0:
            return None
        last_spike = spikes.iloc[-1]
        # Derive composite spike_type string from active mechanism columns

        active_types = [
            name for name, col in self.mechanism_cols if last_spike.get(col, 0) == 1
        ]
        spike_type_str = ",".join(active_types)
        # Convert close_time (ms) to human-readable UTC timestamp (kept under key 'timestamp')
        close_time_val = last_spike.get("close_time", None)
        timestamp_str = None
        if close_time_val is not None:
            close_ms = int(close_time_val)
            timestamp_str = datetime.fromtimestamp(close_ms / 1000, tz=UTC).strftime(
                "%Y-%m-%d %H:%M"
            )
        else:
            timestamp_str = ""

        return {
            "timestamp": timestamp_str,
            "price": last_spike["close"],
            "price_change": last_spike["price_change"],
            "price_change_pct": last_spike["price_change"] * 100,
            "volume": last_spike["volume"],
            "volume_ratio": last_spike["volume_ratio"],
            "quote_asset_volume": last_spike["quote_asset_volume"],
            "spike_type": spike_type_str,
            "rsi": last_spike["rsi"],
            "body_size_pct": last_spike["body_size_pct"],
            "number_of_trades": last_spike["number_of_trades"],
            "price_std": last_spike["price_std"],
            "price_zscore": last_spike["price_zscore"],
            "volume_zscore": last_spike["volume_zscore"],
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
                - ₿ Correlation: {safe_format(self.ti.btc_correlation)}
                - Autotrade?: {"Yes" if autotrade else "No"}
                - ADP diff: {safe_format(adp_diff)} (prev: {safe_format(adp_diff_prev)})
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
                bb_spreads=HABollinguerSpread(
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

        if not last_spike:
            logging.debug("No recent spike detected for breakout.")
            return

        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if (
            current_price > bb_high
            and self.current_market_dominance == MarketDominance.LOSERS
            and adp_diff > 0
            and adp_diff_prev > 0
        ):
            algo = "spike_hunter_breakout"
            autotrade = True

            if self.match_loser(self.ti.symbol):
                algo = "spike_hunter_top_loser"
                autotrade = False

            # Guard against None current_symbol_data (mypy: Optional indexing)
            symbol_data = self.ti.current_symbol_data
            base_asset = symbol_data["base_asset"] if symbol_data else "Base asset"
            quote_asset = symbol_data["quote_asset"] if symbol_data else "Quote asset"

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]}
                - 📈 Price Change: {safe_format(last_spike["price_change"], ".4f")}
                - Number of trades: {last_spike["number_of_trades"]}
                - 📊 {base_asset} volume: {last_spike["volume"]:,.0f}
                - Price z-score: {safe_format(last_spike["price_zscore"], ".2f")}
                - Volume z-score: {safe_format(last_spike["volume_zscore"], ".2f")}
                - 📊 {quote_asset} volume: {last_spike["quote_asset_volume"]:,.0f}
                - 📊 RSI: {safe_format(last_spike["rsi"], ".2f")}
                - 📏 Body Size %: {safe_format(last_spike["body_size_pct"], ".4f")}
                - Number of Trades: {last_spike["number_of_trades"]}
                - ₿ Correlation: {safe_format(self.ti.btc_correlation)}
                - Autotrade?: {"Yes" if autotrade else "No"}
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
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

            return True

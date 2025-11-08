import logging
from datetime import datetime
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from models.signals import HABollinguerSpread, SignalsConsumer
from shared.enums import Strategy
from shared.heikin_ashi import HeikinAshi
from shared.utils import round_numbers, timestamp_to_datetime

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class SpikeHunterV2:
    REQUIRED_BASE_FEATURES = [
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
        "taker_buy_ratio",
        "trades_ratio",
        "trades_zscore",
        "quote_volume_ratio",
    ]
    EARLY_FEATURES = [
        "std_ratio_8_20",
        "vol_ratio_slope_3",
        "vol_ratio_accel",
        "trades_ratio_slope_3",
        "quote_vol_ratio_slope_3",
        "pc_1",
        "pc_2c",
        "pc_3c",
        "pc_pos_count_5",
        "pc_abs_sum_5",
        "taker_buy_ratio",
        "taker_buy_ratio_slope_5",
        "body_size_pct_z",
        "vol_compression_flag",
        "volume_ratio",
        "price_zscore",
    ]

    def __init__(
        self,
        cls: "CryptoAnalytics",
    ):
        self.symbol = cls.symbol
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model_v2.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.bundle = joblib.load(abs_file_path)
        df = cls.clean_df.copy()
        self.df: pd.DataFrame = HeikinAshi.get_heikin_ashi(df)
        self.binbot_api = cls.binbot_api
        self.price_precision = cls.binbot_api.price_precision(symbol=cls.symbol)
        self.current_symbol_data = cls.current_symbol_data
        self.btc_correlation = cls.btc_correlation
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        # Threshold extraction (names preserved)
        self.thresholds = self.bundle.get("thresholds", {})
        self.models = self.bundle.get("models", {})
        self.volume_cluster_min_ratio = self.thresholds.get(
            "volume_cluster_min_ratio", 2.0
        )
        self.volume_cluster_window = self.thresholds.get("volume_cluster_window", 8)
        self.volume_cluster_min_count = self.thresholds.get(
            "volume_cluster_min_count", 2
        )
        self.volume_cluster_label_mode = self.thresholds.get(
            "volume_cluster_label_mode", "last"
        ).lower()  # now supports: last, all, first
        self.price_break_base_threshold = self.thresholds.get(
            "price_break_base_threshold", 0.05
        )
        self.price_break_dynamic_q = self.thresholds.get("price_break_dynamic_q", 0.90)
        self.price_break_use_dynamic = self.thresholds.get(
            "price_break_use_dynamic", True
        )
        self.price_break_auto_tune = self.thresholds.get("price_break_auto_tune", False)
        self.price_break_target_rate = self.thresholds.get(
            "price_break_target_rate", 0.02
        )
        self.price_break_min_quantile = self.thresholds.get(
            "price_break_min_quantile", 0.75
        )
        self.price_break_max_quantile = self.thresholds.get(
            "price_break_max_quantile", 0.985
        )
        self.price_break_smoothing = self.thresholds.get("price_break_smoothing", 0.5)
        self.price_break_auto_lookback = self.thresholds.get(
            "price_break_auto_lookback", 180
        )
        self.use_raw_price_break = self.thresholds.get("use_raw_price_break", False)
        # New compound / acceleration thresholds
        self.cumulative_price_window = self.thresholds.get("cumulative_price_window", 3)
        self.cumulative_price_threshold = self.thresholds.get(
            "cumulative_price_threshold", 0.035
        )  # 3.5% cumulative
        self.accel_volume_deriv_window = self.thresholds.get(
            "accel_volume_deriv_window", 3
        )
        self.accel_volume_deriv_min = self.thresholds.get("accel_volume_deriv_min", 0.8)
        self.accel_price_change_min = self.thresholds.get(
            "accel_price_change_min", 0.02
        )
        # Early proba
        self.early_proba_threshold = self.thresholds.get("early_proba_threshold", 0.45)
        self.early_proba_min_slope = self.thresholds.get("early_proba_min_slope", 0.05)
        self.early_proba_require_volume = self.thresholds.get(
            "early_proba_require_volume", 1.0
        )
        self.require_both_patterns = self.thresholds.get("require_both_patterns", False)
        self.post_spike_cooldown_bars = self.thresholds.get(
            "post_spike_cooldown_bars", 0
        )
        self.require_bullish_spike = self.thresholds.get("require_bullish_spike", True)
        self.body_size_pct_min = self.thresholds.get("body_size_pct_min", 0.0)
        self.lock_thresholds = bool(self.thresholds.get("lock_thresholds", False))
        # Models
        self.current_clf = self.models.get("current_classifier")
        self.early_clf = self.models.get("lead_classifier")
        self.early_proba_augment = self.bundle.get("metadata", {}).get(
            "early_proba_augment", True
        )
        self.early_proba_slope_lookback = 3

    def cleanup(self):
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    def auto_calibrate(
        self,
        volume_quantile: float = 0.985,
        price_base_floor_quantile: float = 0.80,
        min_volume_ratio: float = 1.3,
        min_price_abs_floor: float = 0.02,
    ):
        vols = self.df.get("volume_ratio", pd.Series(dtype=float)).dropna()
        pcs = self.df.get("price_change_abs", pd.Series(dtype=float)).dropna()
        if vols.empty or pcs.empty:
            logging.info("[AutoCalibrate] Missing distribution data; skipping.")
            return
        old_price_floor = self.price_break_base_threshold
        new_vol_thr = float(max(min_volume_ratio, np.quantile(vols, volume_quantile)))
        new_price_floor = float(
            max(min_price_abs_floor, np.quantile(pcs, price_base_floor_quantile))
        )
        self.volume_cluster_min_ratio = new_vol_thr
        self.price_break_base_threshold = max(old_price_floor, new_price_floor)
        if self.price_break_use_dynamic:
            logging.info(
                "[AutoCalibrate] Dynamic price quantile logic will adapt above calibrated floor."
            )

    # -------- Features -------- #
    def compute_base_features(self, window: int = 12):
        if self.df.empty:
            return
        df = self.df
        eff = window
        df["price_change"] = df["close"].pct_change()
        df["price_change_abs"] = df["price_change"].abs()
        if "raw_close" in df.columns:
            df["raw_price_change"] = df["raw_close"].pct_change()
            df["raw_price_change_abs"] = df["raw_price_change"].abs()
        df["body_size"] = (df["close"] - df["open"]).abs()
        df["body_size_pct"] = df["body_size"] / (df["open"] + 1e-6)
        df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
        df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
        df["upper_wick_ratio"] = df["upper_wick"] / (df["body_size"] + 1e-6)
        df["lower_wick_ratio"] = df["lower_wick"] / (df["body_size"] + 1e-6)
        df["total_range"] = df["high"] - df["low"]
        df["range_pct"] = df["total_range"] / (df["open"] + 1e-6)
        df["is_bullish"] = (df["close"] > df["open"]).astype(int)
        df["close_open_ratio"] = (df["close"] - df["open"]) / (df["open"] + 1e-6)
        df["price_ma"] = df["close"].rolling(eff).mean()
        df["price_std"] = df["close"].rolling(eff).std()
        df["price_zscore"] = (df["close"] - df["price_ma"]) / (df["price_std"] + 1e-6)
        df["volume_ma"] = df["volume"].rolling(eff).mean()
        df["volume_ratio"] = df["volume"] / (df["volume_ma"] + 1e-6)
        df["volume_zscore"] = (df["volume"] - df["volume_ma"]) / (
            df["volume"].rolling(eff).std() + 1e-6
        )
        df["quote_volume_ma"] = df["quote_asset_volume"].rolling(eff).mean()
        df["quote_volume_ratio"] = df["quote_asset_volume"] / (
            df["quote_volume_ma"] + 1e-6
        )
        df["trades_ma"] = df["number_of_trades"].rolling(eff).mean()
        df["trades_ratio"] = df["number_of_trades"] / (df["trades_ma"] + 1e-6)
        df["trades_zscore"] = (df["number_of_trades"] - df["trades_ma"]) / (
            df["number_of_trades"].rolling(eff).std() + 1e-6
        )
        df["momentum_3"] = df["close"].pct_change(3)
        df["momentum_5"] = df["close"].pct_change(5)
        df["close_to_high"] = (df["high"] - df["close"]) / (df["high"] + 1e-6)
        df["close_to_low"] = (df["close"] - df["low"] + 1e-6) / (df["close"] + 1e-6)
        if "taker_base" in df.columns:
            df["taker_buy_ratio"] = df["taker_base"] / (df["volume"] + 1e-6)
        else:
            df["taker_buy_ratio"] = 0.0
        self.df = df

    def compute_early_features(self):
        df = self.df.copy()
        df["rolling_price_std_8"] = df["close"].rolling(8).std()
        df["rolling_price_std_20"] = df["close"].rolling(20).std()
        df["std_ratio_8_20"] = df["rolling_price_std_8"] / (
            df["rolling_price_std_20"] + 1e-6
        )
        df["vol_ratio_slope_3"] = df["volume_ratio"].diff(3)
        df["vol_ratio_accel"] = df["vol_ratio_slope_3"].diff()
        df["trades_ratio_slope_3"] = df.get("trades_ratio", 0).diff(3)
        df["quote_vol_ratio_slope_3"] = df.get("quote_volume_ratio", 0).diff(3)
        df["pc_1"] = df["price_change"]
        df["pc_2c"] = df["price_change"].rolling(2).sum()
        df["pc_3c"] = df["price_change"].rolling(3).sum()
        df["pc_pos_count_5"] = (df["price_change"] > 0).rolling(5).sum()
        df["pc_abs_sum_5"] = df["price_change_abs"].rolling(5).sum()
        df["taker_buy_ratio"] = df.get("taker_buy_ratio", 0.0)
        df["taker_buy_ratio_slope_5"] = df["taker_buy_ratio"].diff(5)
        df["body_size_pct_ma_10"] = df["body_size_pct"].rolling(10).mean()
        df["body_size_pct_std_10"] = df["body_size_pct"].rolling(10).std()
        df["body_size_pct_z"] = (df["body_size_pct"] - df["body_size_pct_ma_10"]) / (
            df["body_size_pct_std_10"] + 1e-6
        )
        df["vol_compression_flag"] = (
            df["rolling_price_std_8"] < df["rolling_price_std_20"] * 0.6
        ).astype(int)
        self.df = df

    # -------- Rule Components -------- #
    def volume_cluster_flag(self):
        df = self.df
        cond = df["volume_ratio"] >= self.volume_cluster_min_ratio
        rolling_count = cond.rolling(self.volume_cluster_window, min_periods=1).sum()
        base_flag = (rolling_count >= self.volume_cluster_min_count) & cond
        if self.volume_cluster_label_mode == "last":
            # Mark last element of each contiguous True run. Using comparison avoids
            # fillna on an object-dtype boolean series (prevents FutureWarning).
            flag = base_flag & (~(base_flag.shift(-1) == True))  # noqa: E712
        elif self.volume_cluster_label_mode == "first":
            # Mark first element of each contiguous True run.
            flag = base_flag & (~(base_flag.shift(1) == True))  # noqa: E712
        else:
            flag = base_flag
        self.df["volume_cluster_flag"] = flag.astype(int)

    def price_break_flag(self):
        df = self.df
        price_abs_series = (
            df["raw_price_change_abs"]
            if (self.use_raw_price_break and "raw_price_change_abs" in df.columns)
            else df["price_change_abs"]
        )
        if not self.price_break_use_dynamic:
            thr_series = pd.Series(self.price_break_base_threshold, index=df.index)
        else:
            base_dyn = price_abs_series.rolling(60, min_periods=20).quantile(
                self.price_break_dynamic_q
            )
            if self.price_break_auto_tune:
                q_recent = 1.0 - self.price_break_target_rate
                q_recent = np.clip(
                    q_recent,
                    self.price_break_min_quantile,
                    self.price_break_max_quantile,
                )
                adaptive = price_abs_series.rolling(
                    self.price_break_auto_lookback, min_periods=40
                ).apply(lambda x: np.quantile(x, q_recent), raw=False)
                dyn = (
                    self.price_break_smoothing * base_dyn
                    + (1 - self.price_break_smoothing) * adaptive
                )
            else:
                dyn = base_dyn
            thr_series = pd.Series(
                np.maximum(self.price_break_base_threshold, dyn), index=df.index
            ).ffill()
        self.df["price_break_flag"] = (price_abs_series >= thr_series).astype(int)
        self.df["price_break_threshold_series"] = thr_series

    def cumulative_price_break_flag(self):
        df = self.df
        w = self.cumulative_price_window
        if w <= 1:
            self.df["cumulative_price_break_flag"] = 0
            return
        pos_pc = df["price_change"].clip(lower=0)
        cum_pos = pos_pc.rolling(w).sum()
        vol_cond = (
            (df["volume_ratio"] >= (self.volume_cluster_min_ratio * 0.8))
            .rolling(w)
            .max()
            .astype(bool)
        )
        flag = (cum_pos >= self.cumulative_price_threshold) & vol_cond
        self.df["cumulative_price_break_flag"] = flag.astype(int)

    def acceleration_flag(self):
        df = self.df
        w = self.accel_volume_deriv_window
        vol_deriv = df["volume_ratio"] - df["volume_ratio"].shift(w)
        price_abs_now = (
            df["price_change_abs"]
            if not self.use_raw_price_break
            else df.get("raw_price_change_abs", df["price_change_abs"])
        )
        flag = (
            (vol_deriv >= self.accel_volume_deriv_min)
            & (price_abs_now >= self.accel_price_change_min)
            & (df["price_change"] > 0)
        )
        self.df["accel_spike_flag"] = flag.fillna(False).astype(int)

    def apply_preliminary_label(self):
        self.volume_cluster_flag()
        self.price_break_flag()
        self.cumulative_price_break_flag()
        self.acceleration_flag()
        df = self.df
        if self.require_both_patterns:
            base_combo = (
                (df["volume_cluster_flag"] == 1) & (df["price_break_flag"] == 1)
            ).astype(int)
        else:
            base_combo = (
                (df["volume_cluster_flag"] == 1) | (df["price_break_flag"] == 1)
            ).astype(int)
        aux = (df["cumulative_price_break_flag"] == 1) | (df["accel_spike_flag"] == 1)
        label_pre = (base_combo | aux).astype(int)
        if self.require_bullish_spike:
            label_pre = label_pre & (df["is_bullish"] == 1)
        if self.body_size_pct_min > 0:
            label_pre = label_pre & (df["body_size_pct"] >= self.body_size_pct_min)
        self.df["label_pre"] = label_pre.astype(int)
        self.df["label"] = self.df["label_pre"].copy()

    def compute_early_proba(self):
        if not self.early_proba_augment or self.early_clf is None:
            self.df["early_spike_proba"] = np.nan
            self.df["early_proba_aug_flag"] = 0
            return
        if any(f not in self.df.columns for f in self.EARLY_FEATURES):
            self.compute_early_features()
        feats = self.df[self.EARLY_FEATURES].fillna(0)
        try:
            proba = self.early_clf.predict_proba(feats)[:, 1]
        except Exception:
            proba = np.zeros(len(self.df))
        self.df["early_spike_proba"] = proba
        slope = self.df["early_spike_proba"] - self.df["early_spike_proba"].shift(
            self.early_proba_slope_lookback
        )
        cond = (
            (self.df["early_spike_proba"] >= self.early_proba_threshold)
            & (slope >= self.early_proba_min_slope)
            & (self.df["volume_ratio"] >= self.early_proba_require_volume)
        )
        if self.require_bullish_spike:
            cond &= self.df["is_bullish"] == 1
        if self.body_size_pct_min > 0:
            cond &= self.df["body_size_pct"] >= self.body_size_pct_min
        self.df["early_proba_aug_flag"] = cond.astype(int)
        self.df["label"] = (
            (self.df["label_pre"] == 1) | (self.df["early_proba_aug_flag"] == 1)
        ).astype(int)

    def apply_cooldown(self):
        if self.post_spike_cooldown_bars <= 0:
            self.df["suppressed_label"] = 0
            return
        last_idx = None
        suppressed = 0
        self.df["suppressed_label"] = 0
        for i in self.df.index:
            if self.df.at[i, "label"] == 1:
                if (
                    last_idx is not None
                    and (i - last_idx) <= self.post_spike_cooldown_bars
                ):
                    self.df.at[i, "suppressed_label"] = 1
                    self.df.at[i, "label"] = 0
                    if (
                        self.df.at[i, "early_proba_aug_flag"] == 1
                        and self.df.at[i, "label_pre"] == 0
                    ):
                        self.df.at[i, "early_proba_aug_flag"] = 0
                    suppressed += 1
                else:
                    last_idx = i
        if suppressed:
            logging.info(f"[Cooldown] Suppressed {suppressed} labels")

    def detect_streaks(self, streak_length: int = 3):
        """
        Detect upward and downward spikes based on consecutive green/red candles.
        Returns boolean Series for upward and downward streak spikes.
        """
        green_candles = (self.df["close"] > self.df["open"]).astype(int)
        up_streak = green_candles.rolling(window=streak_length).sum()
        upward_streak = up_streak >= streak_length

        red_candles = (self.df["close"] < self.df["open"]).astype(int)
        down_streak = red_candles.rolling(window=streak_length).sum()
        downward_streak = down_streak >= streak_length
        self.df["upward"] = upward_streak.astype(int)
        self.df["downward"] = downward_streak.astype(int)

    # -------------- Public API -------------- #
    def detect(self) -> pd.DataFrame:
        if self.df.empty:
            return
        self.compute_base_features()
        self.auto_calibrate()
        # Recompute base each pass to keep derivative series consistent
        self.compute_base_features()
        self.apply_preliminary_label()
        self.compute_early_proba()
        self.apply_cooldown()
        self.detect_streaks()
        return self.df

    def latest_signal(self) -> dict | None:
        """
        Return a rich classification summary for the most recent bar.
        Parameters:
            run_detect: when True, force a fresh detect() pass before reading.
        Returns:
            dict including (existing keys preserved + new component flags):
                timestamp, close, label, label_pre, early_proba_aug_flag,
                volume_cluster_flag, price_break_flag,
                cumulative_price_break_flag, accel_spike_flag,
                is_final_spike, is_aug_only, is_suppressed, signal_type, signals
                signal_type values: 'FinalSpike', 'AugOnly', 'Suppressed', or None.
                signals: ordered list of active component signals for this bar
                  (e.g. ['Cumulative', 'Accel', 'FinalSpike']).
        """
        if self.df.empty:
            raise RuntimeError("No data available.")
        is_detected = self.detect()
        if is_detected is None or self.df.empty:
            return None

        row = self.df.iloc[-1]
        signal_type: str = ""
        is_aug_only = bool(
            row.get("label", 0) == 1
            and row.get("label_pre", 0) == 0
            and row.get("early_proba_aug_flag", 0) == 1
        )
        is_supp = bool(row.get("suppressed_label", 0) == 1)

        if is_aug_only:
            signal_type = "AugOnly"
        if is_supp:
            signal_type = "Suppressed"

        # Assemble component signals list (kept consistent with notebook version)
        if row.get("cumulative_price_break_flag", 0) == 1:
            signal_type = "Cumulative"
        if row.get("accel_spike_flag", 0) == 1:
            signal_type = "Acceleration"
        if row.get("price_break_flag", 0) == 1:
            signal_type = "PriceBreak"
        if row.get("volume_cluster_flag", 0) == 1:
            signal_type = "VolumeCluster"

        timestamp = (
            timestamp_to_datetime(row.get("close_time"))
            if row.get("close_time", None)
            else timestamp_to_datetime(int(datetime.now().timestamp() * 1000))
        )

        # threshold below which we may want to consider not trading
        number_trades_thr = self.df["number_of_trades"].quantile(0.75)

        volume = self.df["volume"].iloc[-1] if "volume" in self.df else 0
        quote_asset_volume = (
            self.df["quote_asset_volume"].iloc[-1]
            if "quote_asset_volume" in self.df
            else 0
        )
        number_of_trades = (
            self.df["number_of_trades"].iloc[-1] if "number_of_trades" in self.df else 0
        )

        return {
            "timestamp": timestamp,
            "close": float(row.get("close", 0)),
            "label": int(row.get("label", 0) == 1),
            "label_pre": int(row.get("label_pre", 0) == 1),
            "early_proba_aug_flag": int(row.get("early_proba_aug_flag", 0) == 1),
            # Component flags exposed for richer downstream logic / telemetry
            # Converted to native Python bools for clearer downstream consumption
            "volume_cluster_flag": bool(row.get("volume_cluster_flag", 0) == 1),
            "price_break_flag": bool(row.get("price_break_flag", 0) == 1),
            "cumulative_price_break_flag": bool(
                row.get("cumulative_price_break_flag", 0) == 1
            ),
            "accel_spike_flag": bool(row.get("accel_spike_flag", 0) == 1),
            # Classification meta
            "is_aug_only": is_aug_only,
            "is_suppressed": is_supp,
            "signal_type": signal_type,
            "number_of_trades": int(number_of_trades),
            "number_of_trades_thr": float(number_trades_thr),
            "volume": float(volume),
            "quote_asset_volume": float(quote_asset_volume),
            "upward": bool(row.get("upward", 0) == 1),
            "downward": bool(row.get("downward", 0) == 1),
        }

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        last_spike = self.latest_signal()

        if not last_spike:
            logging.info("No recent spike detected for breakout.")
            return

        # Introduce randomness to spread probability of big spikes
        # got_lucky = random() < 0.5

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if (
            (
                last_spike["cumulative_price_break_flag"]
                or last_spike["is_suppressed"]
                or last_spike["volume_cluster_flag"]
                or last_spike["early_proba_aug_flag"]
                or last_spike["accel_spike_flag"]
            )
            and last_spike["number_of_trades"] > 20
            and last_spike["number_of_trades_thr"] > 0
        ):
            algo = f"spike_hunter_v2_{last_spike['signal_type']}"
            bot_strategy = Strategy.long
            autotrade = True
            symbol_data = self.current_symbol_data

            if last_spike["upward"]:
                streak = "📈"
            elif last_spike["downward"]:
                if symbol_data and not symbol_data["is_margin_trading_allowed"]:
                    logging.info(
                        f"Skipping downward spike for {self.symbol}: margin trading not allowed."
                    )
                    return
                streak = "📉"
                bot_strategy = Strategy.margin_short
                autotrade = False
            else:
                streak = "N/A"
                autotrade = False
                return

            # if not got_lucky:
            #     autotrade = False

            # Guard against None current_symbol_data (mypy: Optional indexing)
            base_asset = symbol_data["base_asset"] if symbol_data else "Base asset"
            quote_asset = symbol_data["quote_asset"] if symbol_data else "Quote asset"

            msg = f"""
                - {streak} [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
                - Current price: {round_numbers(current_price, decimals=self.price_precision)}
                - Last close timestamp: {last_spike["timestamp"]}
                - Number of trades: {last_spike["number_of_trades"]} (thr: {round_numbers(last_spike["number_of_trades_thr"], decimals=self.price_precision)})
                - 📊 {base_asset} volume: {round_numbers(last_spike["volume"], decimals=self.price_precision)}
                - 📊 {quote_asset} volume: {round_numbers(last_spike["quote_asset_volume"], decimals=self.price_precision)}
                - ₿ Correlation: {round_numbers(self.btc_correlation, decimals=self.price_precision)}
                - Autotrade?: {"Yes" if autotrade else "No"}
                - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
                - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
                """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=current_price,
                msg=msg,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=bot_strategy,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.telegram_consumer.send_signal(value.model_dump_json())
            await self.at_consumer.process_autotrade_restrictions(value)

import logging
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from algorithms.heikin_ashi import HeikinAshi
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import Strategy
from shared.utils import safe_format

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
        interval: str = "15m",
        limit: int = 500,
    ):
        self.symbol = cls.symbol
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model_v2.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.bundle = joblib.load(abs_file_path)
        self.interval = interval
        self.limit = limit
        df = cls.clean_df.copy()
        self.df: pd.DataFrame = HeikinAshi.get_heikin_ashi(df)
        self.binbot_api = cls.binbot_api
        self.current_symbol_data = cls.current_symbol_data
        self.btc_correlation = cls.btc_correlation
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        # Threshold extraction (names preserved)
        self.volume_cluster_min_ratio = 2.0
        self.volume_cluster_window = 8
        self.volume_cluster_min_count = 2
        self.volume_cluster_label_mode = "last"
        self.price_break_base_threshold = 0.05
        self.price_break_dynamic_q = 0.90
        self.price_break_use_dynamic = True
        self.price_break_auto_tune = False
        self.price_break_target_rate = 0.02
        self.price_break_min_quantile = 0.75
        self.price_break_max_quantile = 0.985
        self.price_break_smoothing = 0.5
        self.price_break_auto_lookback = 180
        self.early_proba_threshold = 0.45
        self.early_proba_min_slope = 0.05
        self.early_proba_require_volume = 1.0
        self.require_both_patterns = False
        self.post_spike_cooldown_bars = 0
        self.require_bullish_spike = True
        self.body_size_pct_min = 0.0
        # Models
        self.current_clf = None
        self.early_clf = None
        self.early_proba_augment = True
        self.early_proba_slope_lookback = 3

    def cleanup(self):
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    # -------------- Feature Engineering -------------- #
    def compute_base_features(self, window: int = 12):
        if self.df.empty:
            return
        self.cleanup()
        df = self.df
        eff = window
        df["price_change"] = df["close"].pct_change()
        df["price_change_abs"] = df["price_change"].abs()
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

    # -------------- Rule Components -------------- #
    def volume_cluster_flag(self):
        df = self.df
        cond = df["volume_ratio"] >= self.volume_cluster_min_ratio
        rolling_count = cond.rolling(self.volume_cluster_window, min_periods=1).sum()
        flag = (rolling_count >= self.volume_cluster_min_count) & cond
        if self.volume_cluster_label_mode == "last":
            next_flag = flag.shift(-1, fill_value=False)
            flag = flag.astype(bool) & (~next_flag.astype(bool))
        self.df["volume_cluster_flag"] = flag.astype(int)

    def price_break_flag(self):
        df = self.df
        if not self.price_break_use_dynamic:
            thr_series = pd.Series(self.price_break_base_threshold, index=df.index)
        else:
            base_dyn = (
                df["price_change_abs"]
                .rolling(60, min_periods=20)
                .quantile(self.price_break_dynamic_q)
            )
            if self.price_break_auto_tune:
                q_recent = 1.0 - self.price_break_target_rate
                q_recent = np.clip(
                    q_recent,
                    self.price_break_min_quantile,
                    self.price_break_max_quantile,
                )
                adaptive = (
                    df["price_change_abs"]
                    .rolling(self.price_break_auto_lookback, min_periods=40)
                    .apply(lambda x: np.quantile(x, q_recent), raw=False)
                )
                dyn = (
                    self.price_break_smoothing * base_dyn
                    + (1 - self.price_break_smoothing) * adaptive
                )
            else:
                dyn = base_dyn
            thr_series = pd.Series(
                np.maximum(self.price_break_base_threshold, dyn), index=df.index
            ).ffill()
        self.df["price_break_flag"] = (df["price_change_abs"] >= thr_series).astype(int)

    def apply_preliminary_label(self):
        self.volume_cluster_flag()
        self.price_break_flag()
        df = self.df
        if self.require_both_patterns:
            label_pre = (
                (df["volume_cluster_flag"] == 1) & (df["price_break_flag"] == 1)
            ).astype(int)
        else:
            label_pre = (
                (df["volume_cluster_flag"] == 1) | (df["price_break_flag"] == 1)
            ).astype(int)
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
            print(f"[Cooldown] Suppressed {suppressed} labels")

    # -------------- Public API -------------- #
    def detect(self) -> pd.DataFrame:
        if self.df.empty:
            raise RuntimeError("No data loaded; set df or call refresh().")
        self.compute_base_features()
        self.apply_preliminary_label()
        self.compute_early_proba()
        self.apply_cooldown()
        return self.df

    def update_with_df(self, new_df: pd.DataFrame):
        if self.df.empty:
            self.df = new_df.copy()
        else:
            self.df = (
                pd.concat([self.df, new_df])
                .drop_duplicates("timestamp")
                .sort_values("timestamp")
                .reset_index(drop=True)
            )
        if len(self.df) > 1000:
            self.df = self.df.iloc[-1000:].reset_index(drop=True)
        return self.detect()

    def debug_signal_breakdown(self) -> dict:
        needed = {"volume_cluster_flag", "price_break_flag", "label_pre"}
        if not needed.issubset(self.df.columns):
            print("[Debug] Run detect() first.")
            return {}
        d = self.df
        base_or = (
            (d["volume_cluster_flag"] == 1) | (d["price_break_flag"] == 1)
        ).astype(int)
        base_and = (
            (d["volume_cluster_flag"] == 1) & (d["price_break_flag"] == 1)
        ).astype(int)
        chosen = base_and if self.require_both_patterns else base_or
        after_bull = (
            chosen
            if not self.require_bullish_spike
            else (chosen & (d["is_bullish"] == 1)).astype(int)
        )
        after_body = (
            after_bull
            if self.body_size_pct_min <= 0
            else (after_bull & (d["body_size_pct"] >= self.body_size_pct_min)).astype(
                int
            )
        )
        out = {
            "rows": len(d),
            "volume_cluster_flag": int(d["volume_cluster_flag"].sum()),
            "price_break_flag": int(d["price_break_flag"].sum()),
            "base_or": int(base_or.sum()),
            "base_and": int(base_and.sum()),
            "chosen": int(chosen.sum()),
            "after_bull": int(after_bull.sum()),
            "after_body": int(after_body.sum()),
            "pre": int(d["label_pre"].sum()),
            "aug": int(
                d.get("early_proba_aug_flag", pd.Series(0, index=d.index)).sum()
            ),
            "final": int(d["label"].sum()),
            "suppressed": int(
                d.get("suppressed_label", pd.Series(0, index=d.index)).sum()
            ),
        }
        print("[Production Debug]", out)
        return out

    def latest_signal(self, run_detect: bool = False) -> dict:
        """Return classification of the most recent bar.
        Parameters:
            run_detect: when True, force a fresh detect() pass before reading.
        Returns:
            dict with keys:
                timestamp, close, label, label_pre, early_proba_aug_flag,
                is_final_spike, is_aug_only, is_suppressed, signal_type
        signal_type values: 'FinalSpike', 'AugOnly', 'Suppressed', 'None'
        """
        if self.df.empty:
            raise RuntimeError("No data available.")
        if run_detect or "label" not in self.df.columns:
            self.detect()
        row = self.df.iloc[-1]
        is_final = bool(row.get("label", 0) == 1)
        is_aug_only = bool(
            is_final
            and row.get("label_pre", 0) == 0
            and row.get("early_proba_aug_flag", 0) == 1
        )
        is_supp = bool(row.get("suppressed_label", 0) == 1)
        if is_final:
            signal_type = "AugOnly" if is_aug_only else "FinalSpike"
        elif is_supp:
            signal_type = "Suppressed"
        else:
            signal_type = None
        return {
            "timestamp": row.get("timestamp"),
            "close": float(row.get("close", np.nan)),
            "label": int(row.get("label", 0)),
            "label_pre": int(row.get("label_pre", 0)),
            "early_proba_aug_flag": int(row.get("early_proba_aug_flag", 0)),
            "is_final_spike": is_final,
            "is_aug_only": is_aug_only,
            "is_suppressed": is_supp,
            "signal_type": signal_type,
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
            logging.debug("No recent spike detected for breakout.")
            return

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if current_price > bb_high and (
            last_spike["is_aug_only"] or last_spike["is_suppressed"]
        ):
            algo = "spike_hunter_v2"
            autotrade = True

            # Guard against None current_symbol_data (mypy: Optional indexing)
            symbol_data = self.current_symbol_data
            base_asset = symbol_data["base_asset"] if symbol_data else "Base asset"
            quote_asset = symbol_data["quote_asset"] if symbol_data else "Quote asset"

            volume = self.df["volume"].iloc[-1] if "volume" in self.df else 0
            number_of_trades = (
                self.df["number_of_trades"].iloc[-1]
                if "number_of_trades" in self.df
                else 0
            )

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
                - $: +{last_spike["price_change_pct"]}
                - Number of trades: {number_of_trades}
                - 📊 {base_asset} volume: {volume}
                - 📊 {quote_asset} volume: {last_spike["quote_asset_volume"]:,.0f}
                - 📊 RSI: {safe_format(last_spike["rsi"], ".2f")}
                - 📏 Body Size %: {safe_format(last_spike["body_size_pct"], ".4f")}
                - Number of Trades: {last_spike["number_of_trades"]}
                - ₿ Correlation: {safe_format(self.btc_correlation)}
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
                bot_strategy=Strategy.long,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.telegram_consumer.send_signal(value.model_dump_json())
            await self.at_consumer.process_autotrade_restrictions(value)

            return True

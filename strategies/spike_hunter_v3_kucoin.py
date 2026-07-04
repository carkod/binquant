import logging
from datetime import datetime
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING, Any

import numpy as np
from pandas import DataFrame, Series
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    Position,
    SignalsConsumer,
    round_numbers,
    timestamp_to_datetime,
)

from market_regime.models import LiveMarketContext
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class SpikeHunterV3KuCoin:
    """
    KuCoin-compatible Spike Hunter v3:
    - Uses OHLCV + quote volume (turnover)
    - Removes unavailable KuCoin features: taker-based ratios, number_of_trades
    - Keeps thresholds + auto-calibration flow
    """

    MAX_MARKET_STRESS_SCORE = 0.35
    MIN_BREADTH_MOMENTUM_POINTS = 0.0

    def __init__(
        self,
        cls: "ContextEvaluator",
    ):
        self.ti = cls
        self.symbol = cls.symbol
        self.market_type = cls.market_type
        self.df_15m = cls.df_15m.copy()
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.market_breadth_data = cls.market_breadth_data

        # Thresholds (v2-like defaults preserved)
        self.volume_cluster_min_ratio = 1.6
        self.volume_cluster_window = 8
        self.volume_cluster_min_count = 2
        self.volume_cluster_label_mode = "last"  # last | all | first
        self.price_break_base_threshold = 0.03
        self.price_break_dynamic_q = 0.85
        self.price_break_use_dynamic = True
        self.price_break_auto_tune = False
        self.price_break_target_rate = 0.02
        self.price_break_min_quantile = 0.75
        self.price_break_max_quantile = 0.985
        self.price_break_smoothing = 0.5
        self.price_break_auto_lookback = 180
        self.use_raw_price_break = False
        # Compound / acceleration thresholds
        self.cumulative_price_window = 3
        self.cumulative_price_threshold = 0.025
        self.accel_volume_deriv_window = 3
        self.accel_volume_deriv_min = 0.45
        self.accel_price_change_min = 0.015
        self.require_both_patterns = False
        self.post_spike_cooldown_bars = 0
        self.require_bullish_spike = True
        self.body_size_pct_min = 0.0

    @staticmethod
    def _coerce_breadth_value(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not isfinite(parsed):
            return None
        return parsed

    @staticmethod
    def _timestamp_sort_key(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value).timestamp()
        except ValueError:
            return None

    def _ordered_breadth_values(
        self,
        values: list[Any],
        timestamps: list[Any],
        *,
        newest_first: bool,
    ) -> list[float]:
        if len(values) >= 2 and len(timestamps) >= len(values):
            timestamped_values: list[tuple[float, float]] = []
            for timestamp, value in zip(timestamps, values, strict=False):
                sort_key = self._timestamp_sort_key(timestamp)
                breadth_value = self._coerce_breadth_value(value)
                if sort_key is not None and breadth_value is not None:
                    timestamped_values.append((sort_key, breadth_value))

            if len(timestamped_values) >= 2:
                return [
                    breadth_value
                    for _, breadth_value in sorted(
                        timestamped_values, key=lambda item: item[0]
                    )
                ]

        parsed_values = [
            parsed
            for value in values
            if (parsed := self._coerce_breadth_value(value)) is not None
        ]
        if newest_first:
            return list(reversed(parsed_values))
        return parsed_values

    def _breadth_momentum_points(self) -> tuple[float | None, str]:
        market_breadth_data = self.market_breadth_data or {}
        timestamps = market_breadth_data.get("timestamp", [])
        if not isinstance(timestamps, list):
            timestamps = []

        for key, newest_first in (
            ("market_breadth_ma", True),
            ("market_breadth", True),
            ("adp", False),
        ):
            values = market_breadth_data.get(key, [])
            if not isinstance(values, list):
                continue

            breadth_values = self._ordered_breadth_values(
                values=values,
                timestamps=timestamps,
                newest_first=newest_first,
            )
            if len(breadth_values) >= 2:
                return (breadth_values[-1] - breadth_values[-2]) * 100, key

        return None, "unavailable"

    def breadth_momentum_direction(
        self,
        context: LiveMarketContext | None,
    ) -> tuple[Position | None, str]:
        if context is None:
            return None, "market_context_unavailable"

        if context.market_stress_score >= self.MAX_MARKET_STRESS_SCORE:
            return None, "market_stress_too_high"

        momentum_points, source = self._breadth_momentum_points()
        if momentum_points is None:
            return None, "breadth_momentum_unavailable"

        if momentum_points > self.MIN_BREADTH_MOMENTUM_POINTS:
            return Position.long, f"breadth_momentum_up_{source}"
        if momentum_points < -self.MIN_BREADTH_MOMENTUM_POINTS:
            return Position.short, f"breadth_momentum_down_{source}"

        return None, "breadth_momentum_flat"

    @staticmethod
    def symbol_spike_confirms_direction(
        last_spike: dict,
        direction: Position,
    ) -> tuple[bool, str]:
        if direction == Position.long:
            long_flags = (
                last_spike["cumulative_price_break_flag"]
                or last_spike["volume_cluster_flag"]
                or last_spike["accel_spike_flag"]
            )
            if long_flags and last_spike["upward"]:
                return True, "symbol_upward_spike"
            return False, "symbol_upward_spike_missing"

        short_flags = (
            last_spike["cumulative_price_break_short_flag"]
            or last_spike["volume_cluster_flag"]
            or last_spike["accel_spike_short_flag"]
        )
        if short_flags and last_spike["downward"]:
            return True, "symbol_downward_spike"
        return False, "symbol_downward_spike_missing"

    def auto_calibrate(
        self,
        volume_quantile: float = 0.97,
        price_base_floor_quantile: float = 0.75,
        min_volume_ratio: float = 1.15,
        min_price_abs_floor: float = 0.015,
    ):
        vols = self.df_15m.get("volume_ratio", Series(dtype=float)).dropna()
        pcs = self.df_15m.get("price_change_abs", Series(dtype=float)).dropna()
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
        if (
            self.price_break_use_dynamic
            and self.price_break_base_threshold > old_price_floor
        ):
            logging.debug(
                "[AutoCalibrate] Price break floor increased from %.6f to %.6f; "
                "dynamic quantile logic will adapt above it.",
                old_price_floor,
                self.price_break_base_threshold,
            )

    # -------- Features -------- #
    def compute_base_features(self, window: int = 12):
        eff = window
        self.df_15m["price_change"] = self.df_15m["close"].pct_change()
        self.df_15m["price_change_abs"] = self.df_15m["price_change"].abs()
        if "raw_close" in self.df_15m.columns:
            self.df_15m["raw_price_change"] = self.df_15m["raw_close"].pct_change()
            self.df_15m["raw_price_change_abs"] = self.df_15m["raw_price_change"].abs()
        self.df_15m["body_size"] = (self.df_15m["close"] - self.df_15m["open"]).abs()
        self.df_15m["body_size_pct"] = self.df_15m["body_size"] / (
            self.df_15m["open"] + 1e-6
        )
        self.df_15m["upper_wick"] = self.df_15m["high"] - self.df_15m[
            ["close", "open"]
        ].max(axis=1)
        self.df_15m["lower_wick"] = (
            self.df_15m[["close", "open"]].min(axis=1) - self.df_15m["low"]
        )
        self.df_15m["upper_wick_ratio"] = self.df_15m["upper_wick"] / (
            self.df_15m["body_size"] + 1e-6
        )
        self.df_15m["lower_wick_ratio"] = self.df_15m["lower_wick"] / (
            self.df_15m["body_size"] + 1e-6
        )
        self.df_15m["total_range"] = self.df_15m["high"] - self.df_15m["low"]
        self.df_15m["range_pct"] = self.df_15m["total_range"] / (
            self.df_15m["open"] + 1e-6
        )
        self.df_15m["is_bullish"] = (self.df_15m["close"] > self.df_15m["open"]).astype(
            int
        )
        self.df_15m["close_open_ratio"] = (
            self.df_15m["close"] - self.df_15m["open"]
        ) / (self.df_15m["open"] + 1e-6)
        self.df_15m["price_ma"] = self.df_15m["close"].rolling(eff).mean()
        self.df_15m["price_std"] = self.df_15m["close"].rolling(eff).std()
        self.df_15m["price_zscore"] = (
            self.df_15m["close"] - self.df_15m["price_ma"]
        ) / (self.df_15m["price_std"] + 1e-6)
        self.df_15m["volume_ma"] = self.df_15m["volume"].rolling(eff).mean()
        self.df_15m["volume_ratio"] = self.df_15m["volume"] / (
            self.df_15m["volume_ma"] + 1e-6
        )
        self.df_15m["volume_zscore"] = (
            self.df_15m["volume"] - self.df_15m["volume_ma"]
        ) / (self.df_15m["volume"].rolling(eff).std() + 1e-6)
        self.df_15m["quote_volume_ma"] = (
            self.df_15m["quote_asset_volume"].rolling(eff).mean()
        )
        self.df_15m["quote_volume_ratio"] = self.df_15m["quote_asset_volume"] / (
            self.df_15m["quote_volume_ma"] + 1e-6
        )
        self.df_15m["momentum_3"] = self.df_15m["close"].pct_change(3)
        self.df_15m["momentum_5"] = self.df_15m["close"].pct_change(5)
        self.df_15m["close_to_high"] = (self.df_15m["high"] - self.df_15m["close"]) / (
            self.df_15m["high"] + 1e-6
        )
        self.df_15m["close_to_low"] = (
            self.df_15m["close"] - self.df_15m["low"] + 1e-6
        ) / (self.df_15m["close"] + 1e-6)

    def compute_early_features(self):
        self.df_15m["rolling_price_std_8"] = self.df_15m["close"].rolling(8).std()
        self.df_15m["rolling_price_std_20"] = self.df_15m["close"].rolling(20).std()
        self.df_15m["std_ratio_8_20"] = self.df_15m["rolling_price_std_8"] / (
            self.df_15m["rolling_price_std_20"] + 1e-6
        )
        self.df_15m["vol_ratio_slope_3"] = self.df_15m["volume_ratio"].diff(3)
        self.df_15m["vol_ratio_accel"] = self.df_15m["vol_ratio_slope_3"].diff()
        self.df_15m["pc_1"] = self.df_15m["price_change"]
        self.df_15m["pc_2c"] = self.df_15m["price_change"].rolling(2).sum()
        self.df_15m["pc_3c"] = self.df_15m["price_change"].rolling(3).sum()
        self.df_15m["pc_pos_count_5"] = (
            (self.df_15m["price_change"] > 0).rolling(5).sum()
        )
        self.df_15m["pc_abs_sum_5"] = self.df_15m["price_change_abs"].rolling(5).sum()
        self.df_15m["body_size_pct_ma_10"] = (
            self.df_15m["body_size_pct"].rolling(10).mean()
        )
        self.df_15m["body_size_pct_std_10"] = (
            self.df_15m["body_size_pct"].rolling(10).std()
        )
        self.df_15m["body_size_pct_z"] = (
            self.df_15m["body_size_pct"] - self.df_15m["body_size_pct_ma_10"]
        ) / (self.df_15m["body_size_pct_std_10"] + 1e-6)
        self.df_15m["vol_compression_flag"] = (
            self.df_15m["rolling_price_std_8"]
            < self.df_15m["rolling_price_std_20"] * 0.6
        ).astype(int)

    # -------- Rule Components -------- #
    def volume_cluster_flag(self):
        cond = self.df_15m["volume_ratio"] >= self.volume_cluster_min_ratio
        rolling_count = cond.rolling(self.volume_cluster_window, min_periods=1).sum()
        base_flag = (rolling_count >= self.volume_cluster_min_count) & cond
        if self.volume_cluster_label_mode == "last":
            flag = base_flag & (~(base_flag.shift(-1) == True))  # noqa: E712
        elif self.volume_cluster_label_mode == "first":
            flag = base_flag & (~(base_flag.shift(1) == True))  # noqa: E712
        else:
            flag = base_flag
        self.df_15m["volume_cluster_flag"] = flag.astype(int)

    def price_break_flag(self):
        price_abs_series = (
            self.df_15m["raw_price_change_abs"]
            if (
                self.use_raw_price_break
                and "raw_price_change_abs" in self.df_15m.columns
            )
            else self.df_15m["price_change_abs"]
        )
        if not self.price_break_use_dynamic:
            thr_series = Series(
                self.price_break_base_threshold, index=self.df_15m.index
            )
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
            thr_series = Series(
                np.maximum(self.price_break_base_threshold, dyn),
                index=self.df_15m.index,
            ).ffill()
        self.df_15m["price_break_flag"] = (price_abs_series >= thr_series).astype(int)
        self.df_15m["price_break_threshold_series"] = thr_series

    def cumulative_price_break_flag(self):
        w = self.cumulative_price_window
        if w <= 1:
            self.df_15m["cumulative_price_break_flag"] = 0
            self.df_15m["cumulative_price_break_short_flag"] = 0
            return
        pos_pc = self.df_15m["price_change"].clip(lower=0)
        cum_pos = pos_pc.rolling(w).sum()
        neg_pc = self.df_15m["price_change"].clip(upper=0).abs()
        cum_neg = neg_pc.rolling(w).sum()
        vol_cond = (
            (self.df_15m["volume_ratio"] >= (self.volume_cluster_min_ratio * 0.8))
            .rolling(w)
            .max()
            .astype(bool)
        )
        flag = (cum_pos >= self.cumulative_price_threshold) & vol_cond
        flag_short = (cum_neg >= self.cumulative_price_threshold) & vol_cond
        self.df_15m["cumulative_price_break_flag"] = flag.astype(int)
        self.df_15m["cumulative_price_break_short_flag"] = flag_short.astype(int)

    def acceleration_flag(self):
        w = self.accel_volume_deriv_window
        vol_deriv = self.df_15m["volume_ratio"] - self.df_15m["volume_ratio"].shift(w)
        price_abs_now = (
            self.df_15m["price_change_abs"]
            if not self.use_raw_price_break
            else self.df_15m.get(
                "raw_price_change_abs", self.df_15m["price_change_abs"]
            )
        )
        flag = (
            (vol_deriv >= self.accel_volume_deriv_min)
            & (price_abs_now >= self.accel_price_change_min)
            & (self.df_15m["price_change"] > 0)
        )
        flag_short = (
            (vol_deriv >= self.accel_volume_deriv_min)
            & (price_abs_now >= self.accel_price_change_min)
            & (self.df_15m["price_change"] < 0)
        )
        self.df_15m["accel_spike_flag"] = flag.fillna(False).astype(int)
        self.df_15m["accel_spike_short_flag"] = flag_short.fillna(False).astype(int)

    def apply_preliminary_label(self):
        self.volume_cluster_flag()
        self.price_break_flag()
        self.cumulative_price_break_flag()
        self.acceleration_flag()
        if self.require_both_patterns:
            base_combo = (
                (self.df_15m["volume_cluster_flag"] == 1)
                & (self.df_15m["price_break_flag"] == 1)
            ).astype(bool)
        else:
            base_combo = (
                (self.df_15m["volume_cluster_flag"] == 1)
                | (self.df_15m["price_break_flag"] == 1)
            ).astype(bool)
        aux = (self.df_15m["cumulative_price_break_flag"] == 1) | (
            self.df_15m["accel_spike_flag"] == 1
        )
        label_pre = (base_combo | aux).astype(bool)
        if self.require_bullish_spike:
            label_pre = label_pre & (self.df_15m["is_bullish"] == 1)
        if self.body_size_pct_min > 0:
            label_pre = label_pre & (
                self.df_15m["body_size_pct"] >= self.body_size_pct_min
            )
        self.df_15m["label_pre"] = label_pre.astype(bool)
        self.df_15m["label"] = self.df_15m["label_pre"].copy()

        # Short-side parallel label: same volume/price-break magnitude
        # filter, but auxiliary flags are bearish equivalents and the
        # candle must be bearish.
        aux_short = (self.df_15m["cumulative_price_break_short_flag"] == 1) | (
            self.df_15m["accel_spike_short_flag"] == 1
        )
        label_short_pre = (base_combo | aux_short).astype(bool)
        is_bearish = (self.df_15m["close"] < self.df_15m["open"]).astype(int)
        label_short_pre = label_short_pre & (is_bearish == 1)
        if self.body_size_pct_min > 0:
            label_short_pre = label_short_pre & (
                self.df_15m["body_size_pct"] >= self.body_size_pct_min
            )
        self.df_15m["label_short_pre"] = label_short_pre.astype(bool)
        self.df_15m["label_short"] = self.df_15m["label_short_pre"].copy()

    def compute_early_proba(self):
        # Disabled: no ML augmentation in KuCoin variant
        self.df_15m["early_spike_proba"] = np.nan
        self.df_15m["early_proba_aug_flag"] = 0

    def apply_cooldown(self):
        if self.post_spike_cooldown_bars <= 0:
            self.df_15m["suppressed_label"] = 0
            self.df_15m["suppressed_label_short"] = 0
            return
        self.df_15m["suppressed_label"] = 0
        self.df_15m["suppressed_label_short"] = 0
        for label_col, suppressed_col in (
            ("label", "suppressed_label"),
            ("label_short", "suppressed_label_short"),
        ):
            last_idx = None
            suppressed = 0
            for i in self.df_15m.index:
                if self.df_15m.at[i, label_col] == 1:
                    if (
                        last_idx is not None
                        and (i - last_idx) <= self.post_spike_cooldown_bars
                    ):
                        self.df_15m.at[i, suppressed_col] = 1
                        self.df_15m.at[i, label_col] = 0
                        suppressed += 1
                    else:
                        last_idx = i
            if suppressed:
                logging.info(f"[Cooldown] Suppressed {suppressed} {label_col}s")

    def detect_streaks(self, streak_length: int = 3):
        green_candles = (self.df_15m["close"] > self.df_15m["open"]).astype(int)
        up_streak = green_candles.rolling(window=streak_length).sum()
        upward_streak = up_streak >= streak_length

        red_candles = (self.df_15m["close"] < self.df_15m["open"]).astype(int)
        down_streak = red_candles.rolling(window=streak_length).sum()
        downward_streak = down_streak >= streak_length
        self.df_15m["upward"] = upward_streak.astype(int)
        self.df_15m["downward"] = downward_streak.astype(int)

    # -------------- Public API -------------- #
    def detect(self) -> DataFrame | None:
        if self.df_15m.empty:
            return None
        self.compute_base_features()
        self.auto_calibrate()
        self.compute_early_features()
        self.apply_preliminary_label()
        self.compute_early_proba()
        self.apply_cooldown()
        self.detect_streaks()
        return self.df_15m

    def latest_signal(self) -> dict | None:
        is_detected = self.detect()
        if is_detected is None or self.df_15m.empty:
            return None

        row = self.df_15m.iloc[-1]
        is_final = bool(row.get("label", 0) == 1)
        signal_type = "FinalSpike" if is_final else "None"

        timestamp = (
            row.get("timestamp")
            if row.get("timestamp", None)
            else timestamp_to_datetime(int(datetime.now().timestamp() * 1000))
        )

        volume = (
            float(self.df_15m["volume"].iloc[-1]) if "volume" in self.df_15m else 0.0
        )
        quote_asset_volume = (
            float(self.df_15m["quote_asset_volume"].iloc[-1])
            if "quote_asset_volume" in self.df_15m
            else 0.0
        )

        return {
            "timestamp": timestamp,
            "close": float(row.get("close", 0)),
            "label": int(row.get("label", 0) == 1),
            "label_pre": int(row.get("label_pre", 0) == 1),
            "label_short": int(row.get("label_short", 0) == 1),
            "label_short_pre": int(row.get("label_short_pre", 0) == 1),
            "early_proba_aug_flag": 0,
            "volume_cluster_flag": bool(row.get("volume_cluster_flag", 0) == 1),
            "price_break_flag": bool(row.get("price_break_flag", 0) == 1),
            "cumulative_price_break_flag": bool(
                row.get("cumulative_price_break_flag", 0) == 1
            ),
            "accel_spike_flag": bool(row.get("accel_spike_flag", 0) == 1),
            "cumulative_price_break_short_flag": bool(
                row.get("cumulative_price_break_short_flag", 0) == 1
            ),
            "accel_spike_short_flag": bool(row.get("accel_spike_short_flag", 0) == 1),
            "signal_type": signal_type,
            "volume": volume,
            "quote_asset_volume": quote_asset_volume,
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
        # Get the updated df_15m
        self.df_15m = self.ti.df_15m.copy()
        last_spike = self.latest_signal()

        if not last_spike:
            logging.info("No recent spike detected for breakout.")
            return

        algo = "spike_hunter_v3_kucoin"
        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        bot_strategy, market_route = self.breadth_momentum_direction(context)

        if bot_strategy is None:
            logging.info(
                "Spike Hunter skipped %s because market breadth route is %s.",
                self.symbol,
                market_route,
            )
            return

        symbol_confirmed, symbol_route = self.symbol_spike_confirms_direction(
            last_spike=last_spike,
            direction=bot_strategy,
        )
        if not symbol_confirmed:
            logging.info(
                "Spike Hunter skipped %s because %s did not confirm %s.",
                self.symbol,
                symbol_route,
                bot_strategy.value,
            )
            return

        route_reason = f"{market_route}_{symbol_route}"
        autotrade = True

        if bot_strategy == Position.long:
            streak = "📈"
            action_label = "LONG ENTRY"
            rule_intent = (
                "BUY when bullish market breadth is confirmed by an upward spike"
            )
        else:
            streak = "📉"
            action_label = "SHORT ENTRY"
            rule_intent = (
                "SELL when bearish market breadth is confirmed by a downward spike"
            )

        base_asset = self.current_symbol_data.base_asset
        quote_asset = self.current_symbol_data.quote_asset
        kucoin_link, terminal_link = build_links_msg(
            self.ti.config.env,
            self.ti.exchange,
            self.market_type,
            self.symbol,
        )

        msg = f"""
            - {streak} [{getenv("ENV")}] <strong>#spike_hunter_v3_kucoin algorithm</strong> #{self.symbol}
            - Action: {action_label}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: {rule_intent}
            - Candle time: {last_spike["timestamp"]}
            - Volume: {round_numbers(last_spike["volume"], decimals=self.price_precision)} {base_asset}
            - Quote volume: {round_numbers(last_spike["quote_asset_volume"], decimals=self.price_precision)} {quote_asset}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Autotrade route: {route_reason}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            bot_params=BotBase(
                pair=self.symbol,
                name=algo,
                position=bot_strategy,
                market_type=self.market_type,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.ti.dispatch_signal_record(value=value)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

"""SpikeHunterScoring

Advanced spike detection with multi-component scoring.

Differences vs spikehunter_v1:
- Per-mechanism boolean flags (same as refactored v1):
  * spike_ml_classifier
  * spike_price_volume
  * spike_momentum
  * spike_rsi_reversal
- Component score columns (bounded, interpretable):
  * score_price (0-20)
  * score_volume (0-10)
  * score_momentum (0-10)
  * score_rsi (0-8)
  * score_ml (0-15) - uses probability if available (falls back to fixed value)
  * score_confluence (0-9)
  * score_penalty (0 to -10)
  * score_total_raw (pre-clamp sum)
  * spike_score (0-100 final)
- Maintains spike_signal for compatibility.
- Provides explanatory attribution in run_analysis.

Scoring Philosophy:
Additive bounded components allow rapid interpretability and tuning. Each
component is clipped to prevent runaway influence. Final score clamped to 0..100.

Usage:
  scorer = SpikeHunterScoring(analytics_cls)
  result = scorer.run_analysis(max_minutes_ago=60)
  if result and result['spike_score'] >= 70: ...

Tuning Guidance:
- Adjust weight ranges (constants in _compute_components)
- Collect live distribution of spike_score for a week; set operational tiers.

"""

from __future__ import annotations

import logging
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

# Imports for signalling (mirrors spikehunter_v1 usage)
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import MarketDominance, Strategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    from producers.analytics import CryptoAnalytics


class SpikeHunterScoring:
    """Enhanced spike detection with composite scoring.

    Parameters
    ----------
    cls : CryptoAnalytics
        Analytics provider with required attributes:
        - clean_df (DataFrame with ohlcv + close_time, open, high, low, close, volume)
        - current_market_dominance
        - market_breadth_data (dict-like with 'adp' series)
        - btc_correlation (float)
        - btc_price (float or price change context)
        - symbol (str)
        - telegram_consumer (async signal sender)
        - at_consumer (autotrade restrictions processor)

    Attributes
    ----------
    df : pd.DataFrame
        Working dataframe with added features & scores.

    Score Components & Ranges
    -------------------------
    score_price: 0-20
    score_volume: 0-10
    score_momentum: 0-10
    score_rsi: 0-8
    score_ml: 0-15
    score_confluence: 0-9
    score_penalty: 0 to -10
    spike_score: 0-100 (clamped)
    """

    def __init__(self, cls: CryptoAnalytics):
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model_v1.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        try:
            self.model = joblib.load(abs_file_path)
        except Exception:  # pragma: no cover - fallback
            self.model = None
            logger.warning(
                "SpikeHunterScoring: ML model load failed; continuing without ML scores."
            )

        # Base thresholds (can be tuned / dynamic)
        self.momentum_threshold = 0.012
        self.window = 12
        self.rsi_oversold = 30
        self.price_threshold = 0.025
        self.volume_threshold = 2.5

        # dependencies
        self.ti = cls
        self.df = cls.clean_df.copy()
        self.current_market_dominance = cls.current_market_dominance

        # analysis windows (minutes)
        self.time_windows = [15, 30, 60, 120, 240, 300]

    # ---------------- Feature Engineering ---------------- #
    def calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def add_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["close_time"], unit="ms")
        df = df.sort_values(by="timestamp").reset_index(drop=True)
        # Price features
        df["price_change"] = df["close"].pct_change()
        df["price_change_abs"] = df["price_change"].abs()
        df["body_size"] = (df["close"] - df["open"]).abs()
        df["body_size_pct"] = df["body_size"] / (df["open"] + 1e-9)
        df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
        df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
        df["upper_wick_ratio"] = df["upper_wick"] / (df["body_size"] + 1e-6)
        df["lower_wick_ratio"] = df["lower_wick"] / (df["body_size"] + 1e-6)
        df["total_range"] = df["high"] - df["low"]
        df["range_pct"] = df["total_range"] / (df["open"] + 1e-9)
        df["is_bullish"] = (df["close"] > df["open"]).astype(int)
        df["close_open_ratio"] = (df["close"] - df["open"]) / (df["open"] + 1e-9)

        effective_window = min(self.window, max(3, len(df) // 4))
        df["price_ma"] = df["close"].rolling(window=effective_window).mean()
        df["price_std"] = df["close"].rolling(window=effective_window).std()
        df["price_zscore"] = (df["close"] - df["price_ma"]) / (df["price_std"] + 1e-6)

        df["volume_ma"] = df["volume"].rolling(window=effective_window).mean()
        df["volume_ratio"] = df["volume"] / (df["volume_ma"] + 1e-9)
        df["volume_zscore"] = (df["volume"] - df["volume_ma"]) / (
            df["volume"].rolling(window=effective_window).std() + 1e-6
        )

        df["momentum_3"] = df["close"].pct_change(3)
        df["momentum_5"] = df["close"].pct_change(5)

        df["high_low_ratio"] = df["high"] / (df["low"] + 1e-9)
        df["close_to_high"] = (df["high"] - df["close"]) / (df["high"] + 1e-9)
        df["close_to_low"] = (df["close"] - df["low"]) / (df["close"] + 1e-9)

        df["rsi"] = self.calculate_rsi(df["close"])
        return df

    # ---------------- Detection & Scoring ---------------- #
    def _predict_ml(self, X: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            return np.zeros(len(X))
        try:
            # Try probability first
            if hasattr(self.model, "predict_proba"):
                proba = self.model.predict_proba(X)
                # assume positive class is index 1
                return proba[:, 1]
            preds = self.model.predict(X)
            return preds.astype(float)
        except Exception:  # pragma: no cover
            return np.zeros(len(X))

    def _compute_components(
        self,
        row: pd.Series,
        ml_prob: float,
        price_thresh: float,
        recent_changes: pd.Series | None,
        positive_moves: int,
        total_momentum: float,
        recent_spike_count: int,
    ) -> dict[str, float]:
        """Compute bounded component scores for a row.

        Returns dict with component scores.
        """
        scores = {
            "score_price": 0.0,
            "score_volume": 0.0,
            "score_momentum": 0.0,
            "score_rsi": 0.0,
            "score_ml": 0.0,
            "score_confluence": 0.0,
            "score_penalty": 0.0,
        }

        price_change = row.get("price_change", 0.0)
        volume_ratio = row.get("volume_ratio", 0.0)
        rsi = row.get("rsi", 50.0)

        # Price component (cap at 2x threshold -> full 20)
        if price_change > 0 and price_thresh > 0:
            scores["score_price"] = min(price_change / price_thresh, 2.0) * 10
            if scores["score_price"] > 20:
                scores["score_price"] = 20

        # Volume component (cap at k ~ 3.5 ratio)
        k = 3.5
        scores["score_volume"] = min(volume_ratio / k, 1.0) * 10

        # Momentum component (if detection context provided)
        if total_momentum > 0 and price_thresh > 0:
            # Weighted by normalized momentum + positive move proportion
            mom_norm = min(total_momentum / price_thresh, 2.0)
            pos_norm = min(positive_moves / 3, 1.0)
            scores["score_momentum"] = min(mom_norm + 0.5 * pos_norm, 2.0) * 5
            if scores["score_momentum"] > 10:
                scores["score_momentum"] = 10

        # RSI reversal component (only when crossing oversold and recovering)
        if (
            row.name >= 14
            and rsi > self.rsi_oversold
            and recent_changes is not None
            and price_change > 0.008
        ):
            overshoot = max(rsi - self.rsi_oversold, 0)
            scores["score_rsi"] = min(overshoot / 20, 1.0) * 8

        # ML component (probability -> 0-15)
        if ml_prob > 0:
            scores["score_ml"] = min(max(ml_prob, 0.0), 1.0) * 15

        # Confluence (count how many raw mechanisms fired; set later by caller)
        # penalty for spike density
        if recent_spike_count > 0:
            scores["score_penalty"] = -min(recent_spike_count * 2, 10)

        return scores

    def detect_and_score(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy().reset_index(drop=True)
        df = self.add_all_features(df)

        # Initialize flags & scores
        for col in [
            "spike_signal",
            "spike_ml_classifier",
            "spike_price_volume",
            "spike_momentum",
            "spike_rsi_reversal",
        ]:
            df[col] = 0
        score_cols = [
            "score_price",
            "score_volume",
            "score_momentum",
            "score_rsi",
            "score_ml",
            "score_confluence",
            "score_penalty",
            "score_total_raw",
            "spike_score",
        ]
        for c in score_cols:
            df[c] = 0.0

        # Dynamic or fixed thresholds
        price_thresh = self.price_threshold or df["price_change"].quantile(0.95)
        volume_thresh = self.volume_threshold or df["volume_ratio"].quantile(0.90)

        # Feature matrix for ML probability (reuse earlier minimal set)
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
        X = df[feature_cols].fillna(0)
        ml_probs = self._predict_ml(X)

        effective_window = min(self.window, max(3, len(df) // 4))

        # Rolling count for spike density penalty (lookback 30 rows)
        recent_window = 30

        for i in range(effective_window, len(df)):
            row = df.loc[i]
            ml_prob = ml_probs[i]
            price_change = row["price_change"]
            volume_ratio = row["volume_ratio"]
            rsi = row["rsi"]

            # Mechanism triggers (booleans first)
            triggered = []

            # ML classifier threshold (simple 0.5 cut)
            if ml_prob >= 0.5:
                df.loc[i, "spike_ml_classifier"] = 1
                triggered.append("ml")

            # Price + Volume
            if price_change > price_thresh and volume_ratio > volume_thresh:
                df.loc[i, "spike_price_volume"] = 1
                triggered.append("pv")

            # Momentum (using 3-candle window inside last 4 candles region similar to v1)
            recent_changes = df.loc[i - 3 : i, "price_change"]
            positive_moves = (recent_changes > self.momentum_threshold).sum()
            total_momentum = recent_changes.sum()
            if positive_moves >= 2 and total_momentum > price_thresh:
                df.loc[i, "spike_momentum"] = 1
                triggered.append("mom")
            else:
                total_momentum = (
                    0  # ensure momentum score stays low if not triggered logically
                )

            # RSI oversold reversal
            if (
                i >= 14
                and rsi > self.rsi_oversold
                and df.loc[i - 1, "rsi"] <= self.rsi_oversold
                and price_change > 0.008
            ):
                df.loc[i, "spike_rsi_reversal"] = 1
                triggered.append("rsi")

            # Spike density penalty (count prior spikes in lookback window)
            start_idx = max(0, i - recent_window)
            recent_spike_count = int(df.loc[start_idx : i - 1, "spike_signal"].sum())

            # Compute component scores
            scores = self._compute_components(
                row=row,
                ml_prob=ml_prob,
                price_thresh=price_thresh,
                recent_changes=recent_changes,
                positive_moves=positive_moves,
                total_momentum=total_momentum,
                recent_spike_count=recent_spike_count,
            )

            # Confluence based on number of mechanisms triggered
            if triggered:
                scores["score_confluence"] = min((len(triggered) - 1) * 3, 9)

            # Aggregate total
            total_raw = sum(scores.values())
            spike_score = max(0.0, min(100.0, total_raw))

            if triggered:
                df.loc[i, "spike_signal"] = 1

            # Persist scores
            df.loc[i, "score_price"] = scores["score_price"]
            df.loc[i, "score_volume"] = scores["score_volume"]
            df.loc[i, "score_momentum"] = scores["score_momentum"]
            df.loc[i, "score_rsi"] = scores["score_rsi"]
            df.loc[i, "score_ml"] = scores["score_ml"]
            df.loc[i, "score_confluence"] = scores["score_confluence"]
            df.loc[i, "score_penalty"] = scores["score_penalty"]
            df.loc[i, "score_total_raw"] = total_raw
            df.loc[i, "spike_score"] = spike_score

        return df

    # ---------------- Public Interface ---------------- #
    def run_analysis(self, max_minutes_ago: int = 30):
        self.df = self.detect_and_score(self.df)
        self.df.reset_index(drop=True, inplace=True)
        spikes = self.df[self.df["spike_signal"] == 1]
        if spikes.empty:
            return None
        last_spike = spikes.iloc[-1]

        # Active mechanisms summary
        mech_map = {
            "ml_classifier": last_spike.get("spike_ml_classifier", 0),
            "price_volume": last_spike.get("spike_price_volume", 0),
            "momentum": last_spike.get("spike_momentum", 0),
            "rsi_reversal": last_spike.get("spike_rsi_reversal", 0),
        }
        active_types = [m for m, v in mech_map.items() if v == 1]
        spike_type_str = ",".join(active_types)

        current_time = pd.Timestamp.now(tz="UTC")
        spike_time = last_spike["timestamp"]
        if spike_time.tz is None:
            spike_time = spike_time.tz_localize("UTC")
        minutes_ago = (current_time - spike_time).total_seconds() / 60
        if minutes_ago > max_minutes_ago:
            return None

        # Attribution ranking (top contributors)
        contrib_cols = [
            "score_price",
            "score_volume",
            "score_momentum",
            "score_rsi",
            "score_ml",
            "score_confluence",
            "score_penalty",
        ]
        contributions = last_spike[contrib_cols].sort_values(ascending=False).to_dict()

        return {
            "timestamp": last_spike["timestamp"],
            "price": last_spike["close"],
            "price_change": last_spike["price_change"],
            "price_change_pct": last_spike["price_change"] * 100,
            "volume": last_spike["volume"],
            "volume_ratio": last_spike["volume_ratio"],
            "spike_type": spike_type_str,
            "spike_score": last_spike["spike_score"],
            "score_total_raw": last_spike["score_total_raw"],
            "minutes_ago": minutes_ago,
            "components": contributions,
            "active_mechanisms": active_types,
            "rsi": last_spike.get("rsi", None),
            "body_size_pct": last_spike.get("body_size_pct", None),
        }

    def get_spikes(self):
        last_spike = None
        for window in self.time_windows:
            last_spike = self.run_analysis(max_minutes_ago=window)
            if last_spike:
                break
        return last_spike

    # ---------------- Signal Helpers (interface parity with SpikeHunter) ---------------- #
    def match_loser(self, symbol: str):
        for loser in self.ti.top_losers_day:
            if loser.get("symbol") == symbol:
                return True
        return False

    def _format_components(self, components: dict[str, float], top_n: int = 3) -> str:
        ordered = sorted(components.items(), key=lambda x: x[1], reverse=True)
        parts = []
        for k, v in ordered[:top_n]:
            parts.append(f"{k.replace('score_', '')}:{v:.1f}")
        return " / ".join(parts)

    async def spike_hunter_bullish(
        self, current_price: float, bb_high: float, bb_low: float, bb_mid: float
    ):
        last_spike = self.get_spikes()
        if not last_spike:
            return
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
            algo = "spike_hunter_bullish_v2"
            autotrade = True
            comp_str = (
                self._format_components(last_spike["components"])
                if "components" in last_spike
                else ""
            )
            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo}</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]:.2f}% (score: {last_spike["spike_score"]:.1f})
                - 📊 Volume: {last_spike["volume_ratio"]:.2f}x
                - ⚙️ Types: {last_spike["spike_type"]}
                - 🧩 Top: {comp_str}
                - ₿ Corr: {self.ti.btc_correlation:.2f}
                - ADP diff: {adp_diff:.2f} (prev: {adp_diff_prev:.2f})
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
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high, bb_mid=bb_mid, bb_low=bb_low
                ),
            )
            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)
            return True

    async def spike_hunter_breakouts(
        self, current_price: float, bb_high: float, bb_low: float, bb_mid: float
    ):
        last_spike = self.get_spikes()
        if not last_spike:
            return
        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )
        if current_price > bb_high:
            algo = "spike_hunter_breakout_v2"
            autotrade = True
            if self.match_loser(self.ti.symbol):
                algo = "spike_hunter_top_loser_v2"
                autotrade = False
            comp_str = (
                self._format_components(last_spike["components"])
                if "components" in last_spike
                else ""
            )
            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo}</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]:.2f}% (score: {last_spike["spike_score"]:.1f})
                - 📊 Volume: {last_spike["volume_ratio"]:.2f}x
                - ⚙️ Types: {last_spike["spike_type"]}
                - 🧩 Top: {comp_str}
                - ₿ Corr: {self.ti.btc_correlation:.2f}
                - ADP diff: {adp_diff:.2f} (prev: {adp_diff_prev:.2f})
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
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high, bb_mid=bb_mid, bb_low=bb_low
                ),
            )
            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)
            return True

    async def spike_hunter_standard(
        self, current_price: float, bb_high: float, bb_low: float, bb_mid: float
    ):
        last_spike = self.get_spikes()
        if not last_spike:
            return
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
            and self.ti.btc_correlation < 0
            and current_price > bb_high
            and self.ti.btc_price < 0
        ):
            algo = "spike_hunter_standard_v2"
            autotrade = True
            comp_str = (
                self._format_components(last_spike["components"])
                if "components" in last_spike
                else ""
            )
            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo}</strong> #{self.ti.symbol}
                - $: +{last_spike["price_change_pct"]:.2f}% (score: {last_spike["spike_score"]:.1f})
                - 📊 Volume: {last_spike["volume_ratio"]:.2f}x
                - ⚙️ Types: {last_spike["spike_type"]}
                - 🧩 Top: {comp_str}
                - ₿ Corr: {self.ti.btc_correlation:.2f}
                - ADP diff: {adp_diff:.2f} (prev: {adp_diff_prev:.2f})
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
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high, bb_mid=bb_mid, bb_low=bb_low
                ),
            )
            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)
            return True


__all__ = ["SpikeHunterScoring"]

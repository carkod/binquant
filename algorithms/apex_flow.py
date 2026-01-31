import logging
from os import getenv
from typing import TYPE_CHECKING

import numpy as np
from pandas import notna, DataFrame, Series
from pybinbot import Strategy, round_numbers, Indicators
from models.signals import SignalCandidate
from consumers.signal_collector import SignalCollector
from shared.enums import direction_type

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ApexFlow:
    """
    Volatility Compression–Expansion (VCE) algorithm.

    This strategy looks for periods where volatility is unusually low
    (tight Bollinger Bands and depressed ATR), followed by an expansion
    in volatility and volume. The idea is that large moves in crypto
    tend to emerge from quiet, compressed ranges rather than at random.
    """

    def __init__(self, cls: "ContextEvaluator") -> None:
        # Symbol / context
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.binbot_api = cls.binbot_api
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.qty_precision = cls.qty_precision
        self.df: DataFrame = cls.df.copy()
        self.btc_df = cls.btc_df.copy()
        self.signal_collector = SignalCollector()

        # Bollinger compression
        self.bb_period = 20
        self.bb_threshold = 0.06

        # ATR compression
        self.atr_period = 14
        self.atr_lookback = 50
        self.atr_percentile = 0.35

        # Expansion confirmation
        self.atr_expansion_mult = 1.2
        self.volume_mult = 1.15
        self.expansion_lookback = 5

    # ---------- Core VCE components ---------- #
    def detect_volatility_compression(self) -> DataFrame:
        """
        Bollinguer bands, ATR based volatility compression detection
        """
        # --- Bollinger Bands ---
        self.df = Indicators.bollinguer_spreads(self.df, window=self.bb_period)

        self.df["bb_width"] = (self.df["bb_upper"] - self.df["bb_lower"]) / (
            self.df["bb_mid"].abs() + 1e-6
        )

        self.df = Indicators.atr(
            self.df, window=self.atr_period, min_periods=self.atr_period
        )

        # ATR compression threshold (relative, NOT absolute)
        atr_threshold = (
            self.df["ATR"]
            .rolling(self.atr_lookback, min_periods=self.atr_lookback)
            .quantile(self.atr_percentile)
        )

        # --- Compression condition ---
        self.df["compression"] = (self.df["bb_width"] < self.bb_threshold) & (
            self.df["ATR"] < atr_threshold
        )

        return self.df

    def detect_volatility_expansion(self) -> DataFrame:
        if "compression" not in self.df.columns:
            return self.df

        self.df["atr_mean"] = self.df["ATR"].rolling(20).mean()
        self.df["vol_mean"] = self.df["volume"].rolling(20).mean()

        expansion = (self.df["ATR"] > self.df["atr_mean"] * self.atr_expansion_mult) & (
            self.df["volume"] > self.df["vol_mean"] * self.volume_mult
        )

        # Must come AFTER compression
        expansion_after_compression = expansion & (
            self.df["compression"]
            .shift(1)
            .rolling(self.expansion_lookback)
            .max()
            .astype(bool)
        )

        self.df["vce_signal"] = expansion_after_compression
        return self.df

    def classify_vce_direction(self) -> DataFrame:
        if self.df.empty or "vce_signal" not in self.df.columns:
            return self.df

        vce_direction = Series(index=self.df.index, dtype=object)
        vce_direction[self.df["close"] > self.df["close"].shift(1)] = "LONG"
        vce_direction[self.df["close"] < self.df["close"].shift(1)] = "SHORT"
        self.df["vce_direction"] = vce_direction

        self.df.loc[~self.df["vce_signal"], "vce_direction"] = None
        return self.df

    # ---------- Momentum Continuation (MCD) components ---------- #
    def compute_mcd_indicators(self) -> DataFrame:
        if self.df.empty:
            return self.df

        # EMAs for momentum structure
        self.df = Indicators.ema(self.df, column="close", span=9, out_col="ema_fast")
        self.df = Indicators.ema(self.df, column="close", span=21, out_col="ema_slow")

        # RSI (reuse shared implementation)
        self.df = Indicators.rsi(df=self.df)

        return self.df

    def detect_momentum_continuation(
        self, rsi_threshold: int = 55, atr_mult: float = 1.2
    ) -> DataFrame:
        if (
            self.df.empty
            or "ema_fast" not in self.df.columns
            or "ema_slow" not in self.df.columns
            or "ATR" not in self.df.columns
        ):
            return self.df

        momentum = (
            (self.df["close"] > self.df["ema_fast"])
            & (self.df["ema_fast"] > self.df["ema_slow"])
            & (self.df["rsi"] > rsi_threshold)
        )

        atr_ok = self.df["ATR"] > self.df["ATR"].rolling(20).mean() * atr_mult

        self.df["momentum_continue"] = momentum & atr_ok

        self.df["mcd_direction"] = np.where(
            self.df["ema_fast"] > self.df["ema_slow"],
            "LONG",
            "SHORT",
        )

        self.df.loc[~self.df["momentum_continue"], "mcd_direction"] = None

        return self.df

    # ---------- Liquidity Sweep Reversal (LSR) components ---------- #
    def detect_liquidity_sweep_reversal(
        self,
        lookback: int = 20,
        volume_mult: float = 1.8,
        cooldown: int = 10,
    ) -> DataFrame:
        if self.df.empty:
            return self.df

        self.df["prev_high"] = self.df["high"].rolling(lookback).max().shift(1)
        self.df["prev_low"] = self.df["low"].rolling(lookback).min().shift(1)
        self.df["vol_mean"] = self.df["volume"].rolling(20).mean()

        sweep_high = (self.df["high"] > self.df["prev_high"]) & (
            self.df["close"] < self.df["prev_high"]
        )

        sweep_low = (self.df["low"] < self.df["prev_low"]) & (
            self.df["close"] > self.df["prev_low"]
        )

        candle_range = self.df["high"] - self.df["low"]
        body = (self.df["close"] - self.df["open"]).abs()

        strong_rejection = body / (candle_range + 1e-9) > 0.5
        micro_trend_up = self.df["ema_fast"] > self.df["ema_slow"]

        volume_ok = self.df["volume"] > self.df["vol_mean"] * volume_mult
        raw_lsr = (
            (sweep_high | sweep_low) & volume_ok & strong_rejection & micro_trend_up
        )

        # Cooldown: allow only first signal in `cooldown` bars
        recent_lsr = raw_lsr.shift(1).rolling(cooldown).max().fillna(False).astype(bool)

        self.df["lsr_signal"] = raw_lsr & ~recent_lsr

        lsr_direction = Series(index=self.df.index, dtype=object)
        lsr_direction[sweep_low] = "LONG"
        lsr_direction[sweep_high] = "SHORT"
        self.df["lsr_direction"] = lsr_direction

        return self.df

    def low_cap_relative_strength_rotation(
        self,
        lookback: int = 20,
        ma_smooth: int = 5,
        min_relative_strength: float = 1.02,
    ) -> DataFrame:
        """
        Low-Cap Relative Strength Rotation (asset vs BTC benchmark).

        - lookback: periods to compute returns
        - ma_smooth: optional smoothing of relative strength
        - min_relative_strength: threshold to consider rotation attractive
        """
        if self.df.empty or self.btc_df.empty:
            return DataFrame()

        # Compute percentage returns over lookback
        asset_ret = self.df["close"].pct_change(lookback)
        btc_ret = self.btc_df["close"].pct_change(lookback)

        # Relative strength
        rel_strength = asset_ret / (btc_ret + 1e-9)

        # Smooth to reduce noise
        rel_strength_ma = rel_strength.rolling(ma_smooth).mean()

        self.df["rel_strength"] = rel_strength
        self.df["rel_strength_ma"] = rel_strength_ma

        # Generate rotation signal
        self.df["lcrs_signal"] = rel_strength_ma > min_relative_strength

        return self.df

    def compute_trend_quality(self) -> None:
        self.df["ema_slow_slope"] = self.df["ema_slow"].diff(3)
        self.df["volume_ma"] = self.df["volume"].rolling(20).mean()
        self.df["bb_width_ma"] = self.df["bb_width"].rolling(20).mean()

    # ---------- Orchestration ---------- #
    def run_all_detectors(self) -> DataFrame:
        """Run VCE, Momentum Continuation, and LSR detectors in sequence.

        Order:
        1) Volatility Compression → Expansion (VCE)
        2) Momentum Continuation (MCD-style)
        3) Liquidity Sweep Reversal (LSR entries)
        """
        self.detect_volatility_compression()
        self.detect_volatility_expansion()
        self.classify_vce_direction()
        self.compute_mcd_indicators()
        self.compute_trend_quality()
        self.detect_momentum_continuation()
        self.detect_liquidity_sweep_reversal()
        return self.df

    def vce_detector(self) -> DataFrame:
        self.run_all_detectors()

        signals = self.df[self.df["vce_signal"]][
            ["close", "atr", "bb_width", "volume", "vce_direction"]
        ]
        return signals

    def get_trend_bias(self) -> direction_type | None:
        if "ema_fast" not in self.df.columns or "ema_slow" not in self.df.columns:
            return None

        if self.df["ema_fast"].iloc[-1] > self.df["ema_slow"].iloc[-1]:
            return "LONG"
        elif self.df["ema_fast"].iloc[-1] < self.df["ema_slow"].iloc[-1]:
            return "SHORT"
        return None

    def market_regime(self) -> str:
        """
        Determines overall market regime using BTC structure.
        Returns: 'BULL', 'BEAR', or 'CHOP'
        """
        if self.btc_df.empty:
            return "CHOP"

        btc_fast = self.btc_df["close"].ewm(span=9).mean().iloc[-1]
        btc_slow = self.btc_df["close"].ewm(span=21).mean().iloc[-1]

        spread = abs(btc_fast - btc_slow) / btc_slow

        if spread < 0.002:
            return "CHOP"

        return "BULL" if btc_fast > btc_slow else "BEAR"

    def regime_allows(self, direction: str) -> bool:
        regime = self.market_regime()

        # BTC regime alignment
        if direction == "LONG" and regime != "BULL":
            return False

        if direction == "SHORT" and regime != "BEAR":
            return False

        # Relative strength confirmation (if available)
        if "rel_strength_ma" in self.df.columns:
            rs = self.df["rel_strength_ma"].iloc[-1]
            if direction == "LONG" and rs < 1.0:
                return False
            if direction == "SHORT" and rs > 1.0:
                return False

        return True

    def signal_score(
        self,
        has_vce: bool,
        has_mcd: bool,
        has_lsr: bool,
        direction: direction_type | None,
        trend_bias: direction_type | None,
        pattern: str | None = None,
        btc_price_change: float = 0.0,
        btc_correlation: float = 0.0,
    ) -> int:
        score = 0

        if has_lsr:
            score += 2
        if has_mcd:
            score += 1
        if has_vce:
            score += 1
        if trend_bias == direction:
            score += 1

        if pattern == "AGGRESSIVE_MOMO":
            score += 2

        # BTC influence
        if direction == "LONG":
            btc_effect = btc_correlation * btc_price_change
        elif direction == "SHORT":
            btc_effect = -btc_correlation * btc_price_change
        else:
            btc_effect = 0

        btc_score = max(min(int(btc_effect / 0.5), 3), -3)
        score += btc_score

        return score

    def clean_momentum_trend(self, row: Series) -> bool:
        return (
            row["ema_fast"] > row["ema_slow"]
            and row["ema_slow_slope"] > 0
            and row["rsi"] >= 54
            and row["bb_width"] > row["bb_width_ma"]
            and row["volume"] > 1.2 * row["volume_ma"]
        )

    # ---------- Public API ---------- #
    async def signal(
        self,
        current_price: float,
        btc_correlation: float,
        btc_price_change: float,
    ) -> None:
        if self.df is None or self.df.empty:
            logging.info("[VCE] No data available for combined VCE/MCD/LSR signal.")
            return

        self.run_all_detectors()

        row = self.df.iloc[-1]
        recent = self.df.iloc[-3:]

        has_vce = bool(recent["vce_signal"].any())
        has_mcd = bool(recent["momentum_continue"].any())
        has_lsr = bool(recent["lsr_signal"].any())
        has_lsr_direction = notna(row.get("lsr_direction"))
        has_mcd_direction = notna(row.get("mcd_direction"))

        pattern = None
        direction: direction_type | None = None

        # --- LCRS computation ---
        lcrs_df = self.low_cap_relative_strength_rotation()
        last_lcrs_signal = (
            bool(lcrs_df["lcrs_signal"].iloc[-1]) if not lcrs_df.empty else False
        )

        trend_bias = self.get_trend_bias()

        if has_lsr and has_lsr_direction:
            if self.market_regime() in {"CHOP", "BULL"}:
                pattern = "LSR"
                direction = row["lsr_direction"]

        elif (
            has_vce
            and trend_bias == "LONG"
            and self.clean_momentum_trend(row)
            and row.get("prev_high")
            and row["close"] > row["prev_high"]
        ):
            pattern = "AGGRESSIVE_MOMO"
            direction = "LONG"

        elif has_mcd and has_mcd_direction:
            if trend_bias == row["mcd_direction"] and self.regime_allows(trend_bias):
                pattern = "MCD"
                direction = trend_bias

        score = self.signal_score(
            has_vce=has_vce,
            has_mcd=has_mcd,
            has_lsr=has_lsr,
            trend_bias=trend_bias,
            direction=direction,
            pattern=pattern,
            btc_price_change=btc_price_change,
            btc_correlation=btc_correlation,
        )

        if not direction or score < 3:
            return

        algo = f"apex_{pattern.lower()}" if pattern else "apex"
        bot_strategy = Strategy.long
        autotrade = True

        base_asset = self.current_symbol_data["base_asset"]

        msg = f"""
            - {"📈" if direction == "LONG" else "📉"} [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - ATR: {round_numbers(float(row.get("atr", 0.0)), decimals=self.price_precision)}
            - BB width: {round_numbers(float(row.get("bb_width", 0.0)), decimals=self.price_precision)}
            - Score: {score}
        """

        if last_lcrs_signal:
            rel_strength_val = lcrs_df["rel_strength_ma"].iloc[-1]
            msg += f"""
                - Relative Strength (vs BTC): {round_numbers(float(rel_strength_val), 4)}
            """

        # Include other signals context if needed
        if has_mcd:
            msg += f"""
                - EMA Fast: {round_numbers(float(row.get("ema_fast", 0.0)), decimals=self.price_precision)}
                - EMA Slow: {round_numbers(float(row.get("ema_slow", 0.0)), decimals=self.price_precision)}
                - RSI: {round_numbers(float(row.get("rsi", 0.0)), decimals=self.price_precision)}
            """

        if has_lsr:
            msg += f"""
                - Previous high: {round_numbers(float(row.get("prev_high", 0.0)), decimals=self.price_precision)}
                - Previous low: {round_numbers(float(row.get("prev_low", 0.0)), decimals=self.price_precision)}
            """

        msg += f"""
            - 📊 {base_asset} volume: {round_numbers(float(row.get("volume", 0.0)), decimals=self.price_precision)}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='https://www.kucoin.com/trade/{self.kucoin_symbol}'>KuCoin</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
        """

        candidate = SignalCandidate(
            symbol=self.symbol,
            algo=algo,
            direction=direction,
            strategy=bot_strategy,
            autotrade=autotrade,
            score=score,
            current_price=current_price,
            atr=float(row.get("atr", 0.0)),
            bb_width=float(row.get("bb_width", 0.0)),
            volume=float(row.get("volume", 0.0)),
            msg=msg,
        )

        await self.telegram_consumer.send_signal(msg)
        await self.signal_collector.handle(
            candidate=candidate,
            dispatch_function=self.at_consumer.process_autotrade_restrictions,
        )

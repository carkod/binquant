import logging
from os import getenv
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from pybinbot import Strategy

from models.signals import HABollinguerSpread, SignalsConsumer
from shared.indicators import Indicators
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class ApexFlow:
    """
    Volatility Compression–Expansion (VCE) algorithm.

    This strategy looks for periods where volatility is unusually low
    (tight Bollinger Bands and depressed ATR), followed by an expansion
    in volatility and volume. The idea is that large moves in crypto
    tend to emerge from quiet, compressed ranges rather than at random.
    """

    def __init__(self, cls: "CryptoAnalytics") -> None:
        # Symbol / context
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.binbot_api = cls.binbot_api
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.qty_precision = cls.qty_precision
        self.df: pd.DataFrame = cls.df.copy()

        # Volatility compression parameters
        self.bb_period = 20
        self.bb_threshold = 0.055
        self.atr_period = 14
        self.atr_lookback = 35
        self.atr_percentile = 0.35

        # Expansion parameters
        self.atr_expansion_mult = 1.35
        self.volume_mult = 1.2
        self.expansion_lookback = 4

    # ---------- Core VCE components ---------- #
    def detect_volatility_compression(self) -> pd.DataFrame:
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

    def detect_volatility_expansion(self) -> pd.DataFrame:
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

    def classify_vce_direction(self) -> pd.DataFrame:
        if self.df.empty or "vce_signal" not in self.df.columns:
            return self.df

        vce_direction = pd.Series(index=self.df.index, dtype=object)
        vce_direction[self.df["close"] > self.df["bb_upper"].shift(1)] = "LONG"
        vce_direction[self.df["close"] < self.df["bb_lower"].shift(1)] = "SHORT"
        self.df["vce_direction"] = vce_direction

        self.df.loc[~self.df["vce_signal"], "vce_direction"] = None
        return self.df

    # ---------- Momentum Continuation (MCD) components ---------- #
    def compute_mcd_indicators(self) -> pd.DataFrame:
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
    ) -> pd.DataFrame:
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
        self, lookback: int = 20, volume_mult: float = 1.8
    ) -> pd.DataFrame:
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

        volume_ok = self.df["volume"] > self.df["vol_mean"] * volume_mult

        self.df["lsr_signal"] = (sweep_high | sweep_low) & volume_ok

        lsr_direction = pd.Series(index=self.df.index, dtype=object)
        lsr_direction[sweep_low] = "LONG"
        lsr_direction[sweep_high] = "SHORT"
        self.df["lsr_direction"] = lsr_direction

        return self.df

    # ---------- Orchestration ---------- #
    def run_all_detectors(self) -> pd.DataFrame:
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
        self.detect_momentum_continuation()
        self.detect_liquidity_sweep_reversal()
        return self.df

    def vce_detector(self) -> pd.DataFrame:
        self.run_all_detectors()
        if self.df.empty:
            return self.df

        signals = self.df[self.df["vce_signal"]][
            ["close", "atr", "bb_width", "volume", "vce_direction"]
        ]
        return signals

    # ---------- Public API ---------- #
    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ) -> None:
        self.run_all_detectors()

        if self.df is None or self.df.empty:
            logging.info("[VCE] No data available for combined VCE/MCD/LSR signal.")
            return

        row = self.df.iloc[-1]

        has_lsr = bool(row.get("lsr_signal", False))
        has_mcd = bool(row.get("momentum_continue", False))
        has_vce = bool(row.get("vce_signal", False))

        pattern = None
        direction: str | None = None

        # Priority: Liquidity Sweep Reversal > Momentum Continuation > VCE impulse
        if has_lsr and row.get("lsr_direction"):
            pattern = "LSR"
            direction = row.get("lsr_direction")
        elif has_mcd and row.get("mcd_direction"):
            pattern = "MCD"
            direction = row.get("mcd_direction")
        elif has_vce and row.get("vce_direction"):
            pattern = "VCE"
            direction = row.get("vce_direction")

        if not pattern or not direction:
            logging.info("[VCE] No active VCE/MCD/LSR pattern on latest bar.")
            return

        algo = "vce"
        bot_strategy = Strategy.margin_short if direction == "SHORT" else Strategy.long
        autotrade = False

        base_asset = self.current_symbol_data.get("base_asset", "Base asset")

        # Base message with shared stats
        msg = f"""
                - {"📈" if direction == "LONG" else "📉"} [{getenv("ENV")}] <strong>#VCE stack ({pattern})</strong> #{self.symbol}
                - Current price: {round_numbers(current_price, decimals=self.price_precision)}
                - Pattern: {pattern}
                - ATR: {round_numbers(float(row.get("atr", 0.0)), decimals=self.price_precision)}
                - BB width: {round_numbers(float(row.get("bb_width", 0.0)), decimals=self.price_precision)}
        """

        # Momentum continuation context
        if has_mcd:
            msg += f"""
                - EMA Fast: {round_numbers(float(row.get("ema_fast", 0.0)), decimals=self.price_precision)}
                - EMA Slow: {round_numbers(float(row.get("ema_slow", 0.0)), decimals=self.price_precision)}
                - RSI: {round_numbers(float(row.get("rsi", 0.0)), decimals=self.price_precision)}
        """

        # Liquidity sweep context
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

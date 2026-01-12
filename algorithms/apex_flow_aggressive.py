import logging
from os import getenv
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from pybinbot import Strategy, HABollinguerSpread, SignalsConsumer, round_numbers
from pybinbot import Indicators
from algorithms.base_apex_flow import BaseApexFlow

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class ApexFlowAggressive(BaseApexFlow):
    """
    Volatility Compression–Expansion (VCE) algorithm.

    This strategy looks for periods where volatility is unusually low
    (tight Bollinger Bands and depressed ATR), followed by an expansion
    in volatility and volume. The idea is that large moves in crypto
    tend to emerge from quiet, compressed ranges rather than at random.
    """

    def __init__(self, cls: "CryptoAnalytics") -> None:
        super().__init__(cls)
        pass

    def apex_aggressive(self) -> dict | None:
        """
        Aggressive breakout capture.
        Paper trading ONLY.
        """
        self.run_all_detectors()
        row = self.df.iloc[-1]

        # ─────────────────────────
        # 1. HARD REQUIREMENT: VCE
        # ─────────────────────────
        if not row["has_vce"]:
            return None

        direction = row["vce_direction"]  # "LONG" or "SHORT"
        price = row["close"]
        atr = row["atr"]

        # ─────────────────────────
        # 2. SOFT REGIME FILTER
        # ─────────────────────────
        btc_bias = self.get_btc_regime()  # "LONG" | "SHORT" | None

        size_multiplier = 1.0
        if btc_bias and btc_bias != direction:
            size_multiplier = 0.5   # do NOT block, just reduce risk

        # ─────────────────────────
        # 3. MOMENTUM SANITY CHECK
        # ─────────────────────────
        rsi = row["rsi"]

        if direction == "LONG" and rsi < 45:
            return None
        if direction == "SHORT" and rsi > 55:
            return None

        # ─────────────────────────
        # 4. ENTRY (IMMEDIATE)
        # ─────────────────────────
        entry = price

        if direction == "LONG":
            stop = entry - 1.0 * atr
            take_profit = entry + 1.5 * atr
        else:
            stop = entry + 1.0 * atr
            take_profit = entry - 1.5 * atr

        # ─────────────────────────
        # 5. TIME STOP METADATA
        # ─────────────────────────
        return {
            "action": "OPEN",
            "direction": direction,
            "entry": entry,
            "stop_loss": stop,
            "take_profit": take_profit,
            "size_multiplier": size_multiplier,
            "time_stop_candles": 3,   # VERY important
            "mode": "aggressive",
            "reason": "VCE_BREAKOUT",
        }

    # ---------- Public API ---------- #
    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ) -> None:
        if self.df is None or self.df.empty:
            logging.info("[VCE] No data available for combined VCE/MCD/LSR signal.")
            return

        position = self.apex_aggressive()


        # Priority: LSR > MCD > VCE

        # Exit if price re-enters Bollinger Bands
        if position["position"] == "LONG" and row["close"] < row["bb_upper"]:
            return "EXIT_BB_REENTRY"

        if position["position"] == "SHORT" and row["close"] > row["bb_lower"]:
            return "EXIT_BB_REENTRY"

        algo = "volatility_compression_expansion"
        bot_strategy = Strategy.long
        autotrade = True

        base_asset = self.current_symbol_data["base_asset"]
        pattern_text = f" ({pattern})" if pattern else ""

        msg = f"""
            - {"📈" if direction == "LONG" else "📉"} [{getenv("ENV")}] <strong>#APEX_{pattern_text}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - ATR: {round_numbers(float(row.get("atr", 0.0)), decimals=self.price_precision)}
            - BB width: {round_numbers(float(row.get("bb_width", 0.0)), decimals=self.price_precision)}
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

        if not direction:
            return

        score = self.signal_score(
            has_vce=has_vce,
            has_mcd=has_mcd,
            has_lsr=has_lsr,
            trend_bias=trend_bias,
            direction=direction,
        )

        # Require minimum quality
        if score < 3:
            logging.info(
                f"[APEX] Signal rejected | {self.symbol} | score={score} | pattern={pattern}"
            )
            return

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

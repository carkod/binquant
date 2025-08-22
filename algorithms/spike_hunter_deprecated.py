import logging
import os
from os import path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import MarketDominance, Strategy

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
        self.ti = cls
        self.df = cls.df
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
        self.limit = 500

    def match_loser(self, symbol: str):
        """
        Match the symbol with the top losers of the day.
        """
        for loser in self.ti.top_losers_day:
            if loser["symbol"] == symbol:
                return True
        return False

    def fetch_adr_series(self, size=500):
        data = self.ti.binbot_api.get_adr_series(size=size)
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

    def run_analysis(self, df) -> tuple[pd.DataFrame, dict]:
        """Run complete spike analysis for a trading pair."""
        df = self.detect_spikes(df)
        spikes = df[df["spike_signal"] == 1]
        early = df[df["early_signal"] == 1]
        lead_times = df.loc[df["lead_time_candles"].notna(), "lead_time_candles"]
        summary = {
            "total_spikes": int(len(spikes)),
            "total_early": int(len(early)),
            "median_lead_candles": float(lead_times.median())
            if len(lead_times)
            else None,
            "spike_rate": len(spikes) / len(df) if len(df) > 0 else 0,
            "early_rate": len(early) / len(df) if len(df) > 0 else 0,
        }
        return df, summary

    def get_last_spike_details(self, df, symbol="CFXUSDC", max_minutes_ago=30):
        spikes = df[df["early_signal"] == 1]
        if len(spikes) == 0:
            return None
        last_spike = spikes.iloc[-1]
        current_time = pd.Timestamp.now()
        spike_time = pd.to_datetime(last_spike["close_time"])
        minutes_ago = (current_time - spike_time).total_seconds() / 60
        if minutes_ago > max_minutes_ago:
            return None
        return {
            "symbol": symbol,
            "timestamp": last_spike["close_time"],
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

    def detect_spikes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main spike detection algorithm with early detection stage.
        """
        df = df.copy()

        # Basic calculations
        df["price_change"] = df["close"].pct_change()
        df["body_size"] = abs(df["close"] - df["open"])
        df["body_size_pct"] = df["body_size"] / df["open"]

        # Volume analysis with adaptive window
        window = getattr(self, "window", 12)
        effective_window = min(window, len(df) // 4)
        if effective_window < 3:
            effective_window = 3

        df["volume_ma"] = (
            df["volume"].rolling(window=effective_window, min_periods=1).mean()
        )
        df["volume_ratio"] = df["volume"] / (df["volume_ma"] + 1e-10)

        # Rolling highs for breakout precondition
        df["rolling_high"] = df["high"].rolling(window, min_periods=2).max()

        # RSI calculation
        df["rsi"] = self.calculate_rsi(df["close"])

        # Momentum aggregates
        early_lookback = getattr(self, "early_lookback", 5)
        df["cum_price_change_3"] = df["price_change"].rolling(3).sum()
        df["cum_price_change_early"] = df["price_change"].rolling(early_lookback).sum()
        df["volume_ratio_change"] = df["volume_ratio"].diff()

        # Initialize signals
        df["early_signal"] = 0
        df["early_reason"] = ""
        df["spike_signal"] = 0
        df["spike_type"] = ""
        df["signal_strength"] = 0.0
        df["lead_time_candles"] = np.nan  # how many candles between early and confirmed

        # Thresholds and params
        price_threshold = getattr(
            self, "price_threshold", df["price_change"].quantile(0.95)
        )
        volume_threshold = getattr(
            self, "volume_threshold", df["volume_ratio"].quantile(0.90)
        )
        momentum_threshold = getattr(self, "momentum_threshold", 0.02)
        rsi_oversold = getattr(self, "rsi_oversold", 30.0)
        early_price_frac = getattr(self, "early_price_frac", 0.6)
        early_volume_frac = getattr(self, "early_volume_frac", 0.7)
        breakout_buffer = getattr(self, "breakout_buffer", 0.002)
        early_momentum_frac = getattr(self, "early_momentum_frac", 0.5)

        last_early_idx = None

        for i in range(effective_window, len(df)):
            current_price_change = df.loc[i, "price_change"]
            current_volume_ratio = df.loc[i, "volume_ratio"]
            current_rsi = df.loc[i, "rsi"]

            # ---------- CONFIRMED SPIKE LOGIC (original) ----------
            signal = 0
            signal_type = ""
            strength = 0.0

            if (
                current_price_change > price_threshold
                and current_volume_ratio > volume_threshold
            ):
                signal = 1
                signal_type = "price_volume"
                strength = min(current_price_change * current_volume_ratio * 10, 10.0)
            elif i >= 5:
                recent_changes = df.loc[i - 3 : i, "price_change"]
                positive_moves = (recent_changes > momentum_threshold).sum()
                total_momentum = recent_changes.sum()
                if positive_moves >= 2 and total_momentum > price_threshold:
                    signal = 1
                    signal_type = "momentum"
                    strength = min(total_momentum * 20, 8.0)
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

            # Only look for an early signal if we did NOT confirm on this candle
            if signal == 0 and i >= early_lookback:
                cond_a = (
                    current_price_change > price_threshold * early_price_frac
                    and current_volume_ratio > volume_threshold * early_volume_frac
                    and (
                        df.loc[i - 1, "price_change"] > 0
                        or df.loc[i - 2, "price_change"] > 0
                    )
                )
                cond_b = (
                    df.loc[i, "close"]
                    > (df.loc[i, "rolling_high"] * (1 + breakout_buffer))
                    and current_volume_ratio > 1.2
                )
                cond_c = (
                    df.loc[i, "cum_price_change_early"]
                    > price_threshold * early_momentum_frac
                    and df.loc[i - 1, "volume_ratio_change"] > 0
                    and current_volume_ratio > 1.0
                )
                if cond_a or cond_b or cond_c:
                    if cond_a:
                        reason = "partial_threshold"
                    elif cond_b:
                        reason = "breakout"
                    else:
                        reason = "cumulative_momentum"
                    df.loc[i, "early_signal"] = 1
                    df.loc[i, "early_reason"] = reason
                    last_early_idx = i

            if signal == 1 and last_early_idx is not None:
                df.loc[i, "lead_time_candles"] = i - last_early_idx
                last_early_idx = None

            df.loc[i, "spike_signal"] = signal
            df.loc[i, "spike_type"] = signal_type
            df.loc[i, "signal_strength"] = strength

        return df

    async def get_spikes(self):
        """
        Generate a signal based on the spike prediction.
        """
        # Use the current DataFrame from technical indicators
        current_df = self.ti.df

        fresh_df, summary = self.run_analysis(current_df)

        # Check for spikes in different time windows
        time_windows = [5, 15]  # 5 minutes, 15 minutes

        for window in time_windows:
            last_spike = self.get_last_spike_details(fresh_df, max_minutes_ago=window)

            if last_spike:
                break

        return last_spike, summary

    async def spike_hunter_bullish(self, current_price, bb_high, bb_low, bb_mid):
        """
        Detect bullish spikes and send signals.
        """

        last_spike, summary = await self.get_spikes()

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
                - 🔥 [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - 📅 Time: {last_spike["close_time"].strftime("%Y-%m-%d %H:%M")}
                - 📈 Price: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - ⚡ Strength: {last_spike["signal_strength"] / 10:.1f}
                - BTC Correlation: {self.ti.btc_correlation:.2f}
                - Early spikes / rate: {summary["total_early"]}/{summary["early_rate"]:.2f}
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
        last_spike, summary = await self.get_spikes()

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
            self.ti.btc_correlation < 0
            and current_price > bb_high
            and self.ti.btc_price < 0
        ):
            algo = "spike_hunter_breakout"
            autotrade = True

            if self.match_loser(self.ti.symbol) and adp_diff > 0 and adp_diff_prev > 0:
                algo = "spike_hunter_top_loser"
                autotrade = False

            msg = f"""
                - 🔥 [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - 📅 Time: {last_spike["close_time"].strftime("%Y-%m-%d %H:%M")}
                - 📈 Price: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - ⚡ Strength: {last_spike["signal_strength"] / 10:.1f}
                - BTC Correlation: {self.ti.btc_correlation:.2f}
                - Early spikes / rate: {summary["total_early"]}/{summary["early_rate"]:.2f}
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
        last_spike, summary = await self.get_spikes()

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
            self.ti.btc_correlation < 0
            and current_price > bb_high
            and self.ti.btc_price < 0
        ):
            algo = "spike_hunter_breakout"
            autotrade = True

            if self.match_loser(self.ti.symbol) and adp_diff > 0 and adp_diff_prev > 0:
                algo = "spike_hunter_top_loser"
                autotrade = False

            msg = f"""
                - 🔥 [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - 📅 Time: {last_spike["close_time"].strftime("%Y-%m-%d %H:%M")}
                - 📈 Price: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - ⚡ Strength: {last_spike["signal_strength"] / 10:.1f}
                - BTC Correlation: {self.ti.btc_correlation:.2f}
                - Early spikes / rate: {summary["total_early"]}/{summary["early_rate"]:.2f}
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

import os
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.utils import round_numbers

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class HeikinAshi:
    """
    Unlike regular candlesticks (which show raw open-high-low-close data for each period), Heikin Ashi candlesticks apply a smoothing formula to make price trends easier to spot.
    """

    def get_heikin_ashi(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert numeric columns
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("timestamp", inplace=True)

        # -------- Heikin Ashi calculation --------
        ha_df = pd.DataFrame(index=df.index, columns=["open", "high", "low", "close"])
        ha_df["close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

        ha_open = [(df["open"].iloc[0] + df["close"].iloc[0]) / 2]
        for i in range(1, len(df)):
            ha_open.append((ha_open[i - 1] + ha_df["close"].iloc[i - 1]) / 2)
        ha_df["open"] = ha_open

        ha_df["high"] = df[["high"]].join(ha_df[["open", "close"]]).max(axis=1)
        ha_df["low"] = df[["low"]].join(ha_df[["open", "close"]]).min(axis=1)

        ha_df["volume"] = df["volume"]
        return ha_df


class Supertrend(HeikinAshi):
    def __init__(self, cls: "TechnicalIndicators") -> None:
        self.ti = cls
        self.ha_df = self.get_heikin_ashi(self.ti.df)

    def chaikin_money_flow(self, period: int = 20) -> pd.Series:
        mfv = (
            (self.ha_df["close"] - self.ha_df["low"])
            - (self.ha_df["high"] - self.ha_df["close"])
        ) / (self.ha_df["high"] - self.ha_df["low"]).replace(0, np.nan)
        mfv = mfv * self.ha_df["volume"]
        cmf = (
            mfv.rolling(window=period, min_periods=1).sum()
            / self.ha_df["volume"].rolling(window=period, min_periods=1).sum()
        )
        return cmf.fillna(0)

    def supertrend(self, period: int = 200, multiplier: float = 10) -> pd.DataFrame:
        hl2 = (self.ha_df["high"] + self.ha_df["low"]) / 2
        price_range = (
            (self.ha_df["high"] - self.ha_df["low"])
            .rolling(window=period, min_periods=1)
            .mean()
        )
        upperband = hl2 + (multiplier * price_range)
        lowerband = hl2 - (multiplier * price_range)
        supertrend = np.zeros(len(self.ha_df))
        direction = np.ones(len(self.ha_df))
        for i in range(1, len(self.ha_df)):
            if self.ha_df["close"].iloc[i] > upperband.iloc[i - 1]:
                direction[i] = 1
            elif self.ha_df["close"].iloc[i] < lowerband.iloc[i - 1]:
                direction[i] = -1
            else:
                direction[i] = direction[i - 1]
                if direction[i] == 1 and lowerband.iloc[i] < lowerband.iloc[i - 1]:
                    lowerband.iloc[i] = lowerband.iloc[i - 1]
                if direction[i] == -1 and upperband.iloc[i] > upperband.iloc[i - 1]:
                    upperband.iloc[i] = upperband.iloc[i - 1]
            supertrend[i] = (
                lowerband.iloc[i] if direction[i] == 1 else upperband.iloc[i]
            )
        self.ha_df["Supertrend"] = supertrend
        self.ha_df["Supertrend_Dir"] = direction
        return self.ha_df

    def ha_supertrend_signal(
        self,
        supertrend_period=200,
        supertrend_mult=10,
        min_body_pct=0.5,
        trend_confirm=10,
        vol_window=30,
        min_gap=10,
        cmf_period=20,
    ) -> tuple[bool, bool]:
        """
        Heikin Ashi + Supertrend Signal with CMF Confirmation

        Returns a boolean for any of the last 5 matches
        """
        ha_df = self.supertrend(period=supertrend_period, multiplier=supertrend_mult)
        ha_df["CMF"] = self.chaikin_money_flow(period=cmf_period)
        ha_up = ha_df["close"] > ha_df["open"]
        ha_dn = ha_df["close"] < ha_df["open"]
        # Minimum candle body size filter (as % of range)
        body = np.abs(ha_df["close"] - ha_df["open"])
        rng = (ha_df["high"] - ha_df["low"]).replace(0, np.nan)
        body_pct = body / rng
        body_filter = (body_pct > min_body_pct).fillna(False)
        # Trend confirmation: require Supertrend direction to persist for N bars
        trend_up = (
            pd.Series(ha_df["Supertrend_Dir"])
            .rolling(trend_confirm)
            .apply(lambda x: np.all(x == 1), raw=True)
            == 1
        ).fillna(False)
        trend_dn = (
            pd.Series(ha_df["Supertrend_Dir"])
            .rolling(trend_confirm)
            .apply(lambda x: np.all(x == -1), raw=True)
            == 1
        ).fillna(False)
        vol_ma = ha_df["volume"].rolling(vol_window).mean()
        vol_filter = (ha_df["volume"] > vol_ma).fillna(False)
        # CMF confirmation: long only if CMF > 0, short only if CMF < 0
        cmf_long = ha_df["CMF"] > 0
        cmf_short = ha_df["CMF"] < 0
        long = (
            ha_up
            & (ha_df["Supertrend_Dir"] == 1)
            & body_filter
            & trend_up
            & vol_filter
            & cmf_long
        )
        short = (
            ha_dn
            & (ha_df["Supertrend_Dir"] == -1)
            & body_filter
            & trend_dn
            & vol_filter
            & cmf_short
        )
        # Enforce minimum gap between signals
        signal = np.zeros(len(ha_df), dtype=int)
        last_signal_idx = -min_gap
        for i in range(len(ha_df)):
            if long.iloc[i] and (i - last_signal_idx >= min_gap):
                signal[i] = 1
                last_signal_idx = i
            elif short.iloc[i] and (i - last_signal_idx >= min_gap):
                signal[i] = -1
                last_signal_idx = i

        long_signal = bool((signal[-5:] == 1).any()) if len(signal) > 0 else False

        short_signal = bool((signal[-5:] == -1).any()) if len(signal) > 0 else False

        return long_signal, short_signal

    async def signal(
        self, current_price: float, bb_high: float, bb_low: float, bb_mid: float
    ) -> pd.DataFrame:
        long_signal, short_signal = self.ha_supertrend_signal()

        if long_signal:
            algo = "ha_supertrend"
            close_price = self.ha_df["close"].iloc[-1]

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {current_price}
            - Strategy: {self.ti.bot_strategy.value}
            - BTC correlation: {round_numbers(self.ti.btc_correlation)}
            - Autotrade?: {"No"}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=close_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=self.ti.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

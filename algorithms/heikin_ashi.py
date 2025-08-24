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

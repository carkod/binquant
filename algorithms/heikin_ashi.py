from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass


class HeikinAshi:
    """
    Unlike regular candlesticks (which show raw open-high-low-close data for each period), Heikin Ashi candlesticks apply a smoothing formula to make price trends easier to spot.
    """

    def get_heikin_ashi(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert numeric columns
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

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
        ha_df["close_time"] = df["close_time"]
        ha_df["open_time"] = df["open_time"]

        return ha_df

import pandas as pd

from shared.ohlc import OHLCDataFrame


class HeikinAshi:
    """
    Heikin Ashi candle transformation.

    Canonical formulas applied to OHLC data:
        HA_Close = (O + H + L + C) / 4
        HA_Open  = (prev_HA_Open + prev_HA_Close) / 2, seed = (O0 + C0) / 2
        HA_High  = max(H, HA_Open, HA_Close)
        HA_Low   = min(L, HA_Open, HA_Close)

    This version:
      * Works if a 'timestamp' column exists (sorted chronologically first).
      * Does NOT mutate the original dataframe in-place; returns a copy.
      * Validates required columns.
    """

    @staticmethod
    def get_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Validate & coerce using the new type guard helper.
        df = OHLCDataFrame.ensure_ohlc(df)
        work = df.reset_index(drop=True).copy()

        # Compute HA_Close from ORIGINAL OHLC (still intact in 'work').
        # Ensure numeric dtypes (API feeds sometimes deliver strings)
        ohlc_cols = ["open", "high", "low", "close"]
        for c in ohlc_cols:
            # Only attempt conversion if dtype is not already numeric
            if not pd.api.types.is_numeric_dtype(work[c]):
                work.loc[:, c] = pd.to_numeric(work[c], errors="coerce")

        if work[ohlc_cols].isna().any().any():
            # Drop rows that became NaN after coercion (invalid numeric data)
            work = work.dropna(subset=ohlc_cols).reset_index(drop=True)
            if work.empty:
                raise ValueError("All OHLC rows became NaN after numeric coercion.")

        ha_close = (work["open"] + work["high"] + work["low"] + work["close"]) / 4.0

        # Seed HA_Open with original O & C (not HA close).
        ha_open = ha_close.copy()
        ha_open.iloc[0] = (work["open"].iloc[0] + work["close"].iloc[0]) / 2.0
        for i in range(1, len(work)):
            ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2.0

        # High / Low derived from max/min of (raw high/low, ha_open, ha_close)
        ha_high = pd.concat([work["high"], ha_open, ha_close], axis=1).max(axis=1)
        ha_low = pd.concat([work["low"], ha_open, ha_close], axis=1).min(axis=1)

        # Assign transformed values.
        work.loc[:, "open"] = ha_open
        work.loc[:, "high"] = ha_high
        work.loc[:, "low"] = ha_low
        work.loc[:, "close"] = ha_close

        return work

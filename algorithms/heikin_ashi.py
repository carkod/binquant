from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass


class HeikinAshi:
    """Heikin Ashi candle transformation.

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

    REQUIRED_COLUMNS = {"open", "high", "low", "close"}

    @staticmethod
    def get_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        missing = HeikinAshi.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns for Heikin Ashi: {missing}")

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "number_of_trades",
            "taker_base",
            "taker_quote",
        ]
        for col in numeric_cols:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # If all quote_asset_volume become NaN after coercion, raise early
        if (
            "quote_asset_volume" in df.columns
            and df["quote_asset_volume"].notna().sum() == 0
        ):
            raise ValueError(
                "quote_asset_volume column is entirely non-numeric after coercion; cannot compute quote_volume_ratio"
            )

        # Work on a copy & ensure chronological order if timestamp provided.
        if "timestamp" in df.columns:
            work = df.sort_values("timestamp").reset_index(drop=True).copy()
        else:
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

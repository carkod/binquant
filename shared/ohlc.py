from __future__ import annotations

from typing import TypeGuard

import pandas as pd


class OHLCDataFrame(pd.DataFrame):
    """DataFrame subclass marking validated OHLC + extended market columns.

    Provides class methods to (a) validate & coerce a generic DataFrame into an
    OHLCDataFrame and (b) act as a type guard for flow-sensitive typing.

    Required columns are kept in the explicit order requested by the user.
    """

    REQUIRED_COLUMNS = [
        "open",
        "high",
        "low",
        "close",
        "open_time",
        "close_time",
        "volume",
        "quote_asset_volume",
    ]

    OPTIONAL_COLUMNS = [
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]

    # Preserve subclass through pandas operations when possible.
    @property
    def _constructor(self):
        return OHLCDataFrame

    @classmethod
    def is_ohlc_dataframe(cls, df: pd.DataFrame) -> TypeGuard[OHLCDataFrame]:
        """Return True if all required columns are present.

        This does *not* guarantee dtypes—only presence. Use `ensure_ohlc` for
        full validation + coercion.
        """
        return set(cls.REQUIRED_COLUMNS).issubset(df.columns)

    @classmethod
    def ensure_ohlc(cls, df: pd.DataFrame) -> OHLCDataFrame:
        """Validate & coerce a DataFrame into an OHLCDataFrame.

        Steps:
        - Verify all REQUIRED_COLUMNS are present (raises ValueError if missing).
        - Coerce numeric columns (including *_time which are expected as ms epoch).
        - Perform early failure if quote_asset_volume becomes entirely NaN.
        - Return the same underlying object cast to OHLCDataFrame (no deep copy).
        """
        missing = set(cls.REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required OHLC columns: {missing}")

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "open_time",
            "close_time",
            "volume",
            "quote_asset_volume",
        ]

        if len(df.columns) >= len(cls.REQUIRED_COLUMNS):
            numeric_cols += [col for col in cls.OPTIONAL_COLUMNS if col in df.columns]

        for col in numeric_cols:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if (
            "quote_asset_volume" in df.columns
            and df["quote_asset_volume"].notna().sum() == 0
        ):
            raise ValueError(
                "quote_asset_volume column is entirely non-numeric after coercion; cannot compute quote_volume_ratio"
            )

        if not isinstance(df, OHLCDataFrame):
            df = OHLCDataFrame(df)
        return df

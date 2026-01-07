import pandas as pd
import pytest

from pybinbot import OHLCDataFrame


def make_base_df():
    return pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0],
            "high": [1.5, 2.5, 3.5],
            "low": [0.5, 1.5, 2.5],
            "close": [1.2, 2.2, 3.2],
            "open_time": [1000, 2000, 3000],
            "close_time": [1500, 2500, 3500],
            "volume": [10, 20, 30],
            "quote_asset_volume": [100, 200, 300],
            "number_of_trades": [1, 2, 3],
            # Updated required taker buy volume columns
            "taker_buy_base_asset_volume": [5, 10, 15],
            "taker_buy_quote_asset_volume": [50, 100, 150],
        }
    )


def test_is_ohlc_dataframe_positive():
    df = make_base_df()
    assert OHLCDataFrame.is_ohlc_dataframe(df) is True


def test_is_ohlc_dataframe_negative():
    """
    Test that a DataFrame missing required OHLC columns is not recognized as such.
    """
    df = make_base_df().drop(columns=["close_time"])
    assert OHLCDataFrame.is_ohlc_dataframe(df) is False


def test_ensure_ohlc_success():
    df = make_base_df()
    validated = OHLCDataFrame.ensure_ohlc(df)
    assert isinstance(validated, OHLCDataFrame)
    # All required columns still present
    for col in OHLCDataFrame.REQUIRED_COLUMNS:
        assert col in validated.columns


def test_ensure_ohlc_missing_columns():
    # Remove two required columns to trigger validation error
    df = make_base_df().drop(columns=["volume", "close_time"])
    with pytest.raises(ValueError) as exc:
        OHLCDataFrame.ensure_ohlc(df)
    msg = str(exc.value)
    assert "volume" in msg and "close_time" in msg


def test_ensure_ohlc_coercion():
    # Provide numeric columns as strings (should be coerced)
    df = make_base_df().astype(
        {
            "open": "string",
            "high": "string",
            "low": "string",
            "close": "string",
        }
    )
    validated = OHLCDataFrame.ensure_ohlc(df)
    for col in ["open", "high", "low", "close"]:
        assert pd.api.types.is_numeric_dtype(validated[col])


def test_quote_asset_volume_all_nan():
    df = make_base_df()
    df["quote_asset_volume"] = ["x", "y", "z"]  # coercion -> all NaN
    with pytest.raises(ValueError) as exc:
        OHLCDataFrame.ensure_ohlc(df)
    assert "quote_asset_volume" in str(exc.value)

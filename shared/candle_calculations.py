"""Independent candle indicator calculations."""

from typing import Any

from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import Candles, ExchangeId, Indicators, KlineSchema


def calculate_candles(
    exchange: ExchangeId, candles: list[list[Any]]
) -> TypedDataFrame[KlineSchema]:
    """Build and enrich a frame owned exclusively by this calculation."""
    df = Candles(exchange=exchange, candles=candles).pre_process()
    if df.empty or df.close.size == 0:
        return df

    for period in (7, 25, 100):
        df = Indicators.moving_averages(df, period)
    df = Indicators.macd(df=df)
    df = Indicators.rsi(df=df)
    df = Indicators.ma_spreads(df)
    df = Indicators.bollinguer_spreads(df)
    df = Indicators.set_twap(df)
    return Indicators.atr(df=df, window=14)

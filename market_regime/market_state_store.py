from typing import Any
from collections.abc import Mapping
from pandas import DataFrame, Series, concat, to_numeric


class MarketStateStore:
    """
    Keeps rolling closed-candle history for every tracked symbol.

    The store is deliberately strategy-agnostic and only manages the shared
    in-memory market state used by the market-context layer.
    """

    def __init__(self, max_bars_per_symbol: int = 200) -> None:
        self.max_bars_per_symbol = max_bars_per_symbol
        self._histories: dict[str, DataFrame] = {}
        self._last_closed_timestamp: dict[str, int] = {}

    def update(
        self,
        symbol: str,
        candle: Mapping[str, Any] | Series | DataFrame,
    ) -> DataFrame:
        normalized = self._normalize_input(candle)
        history = self._histories.get(symbol, DataFrame())
        history = concat([history, normalized], ignore_index=True)
        history = history.drop_duplicates(subset=["timestamp"], keep="last")
        history = history.sort_values("timestamp").tail(self.max_bars_per_symbol)
        history = history.reset_index(drop=True)
        self._histories[symbol] = history
        self._last_closed_timestamp[symbol] = int(history.iloc[-1]["timestamp"])
        return history.copy()

    def get_symbol_history(self, symbol: str) -> DataFrame:
        history = self._histories.get(symbol)
        if history is None:
            return DataFrame()
        return history.copy()

    def get_all_histories(self) -> dict[str, DataFrame]:
        return {symbol: history.copy() for symbol, history in self._histories.items()}

    def get_last_closed_timestamp(self, symbol: str) -> int | None:
        return self._last_closed_timestamp.get(symbol)

    def get_tracked_symbols(self) -> list[str]:
        return sorted(self._histories.keys())

    def get_fresh_symbols(self, timestamp: int) -> set[str]:
        return {
            symbol
            for symbol, closed_timestamp in self._last_closed_timestamp.items()
            if closed_timestamp == timestamp
        }

    @staticmethod
    def _normalize_input(
        candle: Mapping[str, Any] | Series | DataFrame,
    ) -> DataFrame:
        if isinstance(candle, DataFrame):
            df = candle.copy()
        elif isinstance(candle, Series):
            df = candle.to_frame().T
        else:
            df = DataFrame([dict(candle)])

        if "timestamp" not in df.columns:
            raise ValueError("MarketStateStore.update requires a 'timestamp' column.")

        required = ["timestamp", "open", "high", "low", "close", "volume"]
        for column in required:
            if column not in df.columns:
                if column == "volume":
                    df[column] = 0.0
                elif column == "open":
                    df[column] = df["close"]
                elif column in {"high", "low"}:
                    df[column] = df["close"]
                else:
                    raise ValueError(f"Missing required candle field '{column}'.")

        for column in required:
            df[column] = to_numeric(df[column], errors="coerce")

        df = df.dropna(subset=["timestamp", "close"])
        df["timestamp"] = df["timestamp"].astype(int)
        return df[required].copy()

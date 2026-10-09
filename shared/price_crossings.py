from math import isfinite

from pandas import DataFrame, to_numeric

CHOP_LOOKBACK_HOURS = 6
CHOP_CROSSING_THRESHOLD = 3


def price_crossings_six_hours(
    df: DataFrame | None, current_price: float, *, now_ms: float, interval_ms: int
) -> int | None:
    """Count close-to-close changes of side around the fixed current price.

    Require consecutive completed candles covering six hours before the
    current bar. Ignore wicks and exact touches; touches preserve the previous
    side. Return None for incomplete, stale, or invalid history.
    """
    if (
        not isfinite(current_price)
        or current_price <= 0
        or df is None
        or not {"open_time", "close_time", "close"}.issubset(df.columns)
    ):
        return None

    lookback_ms = CHOP_LOOKBACK_HOURS * 60 * 60 * 1000
    if interval_ms <= 0 or lookback_ms % interval_ms:
        return None

    window_end = int(now_ms // interval_ms) * interval_ms
    window_start = window_end - lookback_ms
    open_times = to_numeric(df["open_time"], errors="coerce")
    close_times = to_numeric(df["close_time"], errors="coerce")
    in_window = (open_times >= window_start) & (open_times < window_end)
    window = df.loc[in_window]
    if open_times.loc[in_window].tolist() != list(
        range(window_start, window_end, interval_ms)
    ):
        return None
    window_close_times = close_times.loc[in_window]
    if not (
        (window_close_times < now_ms)
        & (window_close_times >= open_times.loc[in_window])
        & (window_close_times <= open_times.loc[in_window] + interval_ms)
    ).all():
        return None
    closes = to_numeric(window["close"], errors="coerce")
    if not closes.map(isfinite).all() or (closes <= 0).any():
        return None

    previous_side = 0
    crossings = 0
    for close in closes:
        side = 1 if close > current_price else -1 if close < current_price else 0
        if side == 0:
            continue
        if previous_side and side != previous_side:
            crossings += 1
        previous_side = side
    return crossings

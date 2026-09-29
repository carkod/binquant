from dataclasses import dataclass
from math import isfinite

from pandas import DataFrame, to_numeric
from pybinbot import Position, round_numbers


@dataclass(frozen=True)
class WeeklyStructureProtection:
    resistance: float
    support: float
    stop_loss_price: float
    stop_loss_pct: float
    trailing_profit_pct: float
    trailing_deviation_pct: float
    candle_count: int


WEEKLY_STRUCTURE_CANDLES = 7 * 24
BOUNDARY_BUFFER_PCT = 0.25
MIN_TRAILING_PROFIT_PCT = 4.5
MAX_TRAILING_PROFIT_PCT = 8.0
MIN_TRAILING_DEVIATION_PCT = 3.0
MAX_TRAILING_DEVIATION_PCT = 6.0
MIN_TRAILING_GAP_PCT = 1.0


def weekly_structure_protection(
    candles: DataFrame,
    *,
    current_price: float,
    position: Position,
    price_precision: int,
) -> WeeklyStructureProtection | None:
    """Build stops from seven days of completed 1h price structure.

    The stop sits just beyond the observed weekly extreme. Trailing settings
    scale with the weekly high-low range, but are bounded so the trail is
    deliberately looser without becoming effectively disabled.
    """
    if (
        not isfinite(current_price)
        or current_price <= 0
        or not {"high", "low"}.issubset(candles.columns)
        or len(candles) < WEEKLY_STRUCTURE_CANDLES
    ):
        return None

    weekly_candles = candles.tail(WEEKLY_STRUCTURE_CANDLES)
    highs = to_numeric(weekly_candles["high"], errors="coerce")
    lows = to_numeric(weekly_candles["low"], errors="coerce")
    if highs.isna().any() or lows.isna().any():
        return None

    resistance = float(highs.max())
    support = float(lows.min())
    if (
        not isfinite(resistance)
        or not isfinite(support)
        or support <= 0
        or resistance <= support
    ):
        return None

    buffer_ratio = BOUNDARY_BUFFER_PCT / 100
    if position == Position.short:
        stop_loss_price = round_numbers(
            resistance * (1 + buffer_ratio), price_precision
        )
        stop_loss_pct = ((stop_loss_price / current_price) - 1) * 100
    else:
        stop_loss_price = round_numbers(support * (1 - buffer_ratio), price_precision)
        stop_loss_pct = (1 - (stop_loss_price / current_price)) * 100

    if stop_loss_price <= 0 or stop_loss_pct <= 0 or stop_loss_pct > 101:
        return None

    weekly_range_pct = ((resistance - support) / current_price) * 100
    trailing_profit_pct = min(
        MAX_TRAILING_PROFIT_PCT,
        max(MIN_TRAILING_PROFIT_PCT, weekly_range_pct * 0.25),
    )
    trailing_deviation_pct = min(
        MAX_TRAILING_DEVIATION_PCT,
        max(MIN_TRAILING_DEVIATION_PCT, weekly_range_pct * 0.20),
        trailing_profit_pct - MIN_TRAILING_GAP_PCT,
    )

    return WeeklyStructureProtection(
        resistance=round_numbers(resistance, price_precision),
        support=round_numbers(support, price_precision),
        stop_loss_price=stop_loss_price,
        stop_loss_pct=round_numbers(stop_loss_pct, 4),
        trailing_profit_pct=round_numbers(trailing_profit_pct, 2),
        trailing_deviation_pct=round_numbers(trailing_deviation_pct, 2),
        candle_count=WEEKLY_STRUCTURE_CANDLES,
    )

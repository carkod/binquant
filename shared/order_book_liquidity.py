"""Entry liquidity policy for KuCoin linear USDT futures.

Depth is executable quote notional: price * contracts * contract multiplier.
The top 100 levels give a conservative lower bound when the band is deeper.
These initial thresholds are safety limits, not calibrated return predictors.
"""

from dataclasses import dataclass
from math import isclose, isfinite

from kucoin_universal_sdk.generate.futures.market import GetPartOrderBookResp


MAX_SPREAD_PCT = 0.3
DEPTH_BAND_PCT = 0.5
MIN_SIDE_DEPTH_USDT = 5_000.0
DEPTH_TO_POSITION_RATIO = 20.0
MAX_BOOK_AGE_SECONDS = 10.0
MAX_FUTURE_SKEW_SECONDS = 2.0


@dataclass(frozen=True)
class LiquidityAssessment:
    allowed: bool
    reason: str
    spread_pct: float | None = None
    bid_depth_usdt: float | None = None
    ask_depth_usdt: float | None = None
    required_side_depth_usdt: float | None = None
    book_timestamp_ns: int | None = None
    max_spread_pct: float = MAX_SPREAD_PCT
    depth_band_pct: float = DEPTH_BAND_PCT


def assess_futures_order_book(
    book: GetPartOrderBookResp,
    *,
    symbol: str,
    multiplier: float,
    position_notional: float,
    now_seconds: float,
) -> LiquidityAssessment:
    """Require fresh, ordered liquidity on both the entry and exit sides."""
    if (
        not isfinite(multiplier)
        or multiplier <= 0
        or not isfinite(position_notional)
        or position_notional <= 0
    ):
        return LiquidityAssessment(False, "invalid_contract_or_position_size")
    if book.symbol != symbol:
        return LiquidityAssessment(False, "order_book_symbol_mismatch")
    if book.ts is None or not isfinite(now_seconds):
        return LiquidityAssessment(False, "order_book_timestamp_missing")
    age = now_seconds - book.ts / 1_000_000_000
    if not -MAX_FUTURE_SKEW_SECONDS <= age <= MAX_BOOK_AGE_SECONDS:
        return LiquidityAssessment(False, "order_book_stale_or_future")

    bids, asks = book.bids, book.asks
    if not bids or not asks:
        return LiquidityAssessment(False, "order_book_side_empty")
    # Optional SDK fields are an external boundary. Reject malformed snapshots
    # rather than ignoring bad levels and potentially inflating available depth.
    for levels in (bids, asks):
        if any(
            len(level) != 2 or any(not isfinite(v) or v <= 0 for v in level)
            for level in levels
        ):
            return LiquidityAssessment(False, "order_book_levels_invalid")
    if any(a[0] <= b[0] for a, b in zip(bids, bids[1:], strict=False)) or any(
        a[0] >= b[0] for a, b in zip(asks, asks[1:], strict=False)
    ):
        return LiquidityAssessment(False, "order_book_levels_unordered")

    best_bid, best_ask = bids[0][0], asks[0][0]
    if best_bid >= best_ask:
        return LiquidityAssessment(False, "order_book_crossed")
    mid = (best_bid + best_ask) / 2
    spread = (best_ask - best_bid) / mid * 100
    bid_depth = sum(
        price * contracts * multiplier
        for price, contracts in bids
        if price >= mid * (1 - DEPTH_BAND_PCT / 100)
    )
    ask_depth = sum(
        price * contracts * multiplier
        for price, contracts in asks
        if price <= mid * (1 + DEPTH_BAND_PCT / 100)
    )
    required_depth = max(
        MIN_SIDE_DEPTH_USDT, position_notional * DEPTH_TO_POSITION_RATIO
    )
    if not all(isfinite(v) for v in (spread, bid_depth, ask_depth, required_depth)):
        return LiquidityAssessment(False, "order_book_notional_invalid")
    reason = "sufficient_depth"
    if spread > MAX_SPREAD_PCT and not isclose(spread, MAX_SPREAD_PCT, rel_tol=1e-12):
        reason = "spread_too_wide"
    elif bid_depth < required_depth and not isclose(bid_depth, required_depth):
        reason = "insufficient_bid_depth"
    elif ask_depth < required_depth and not isclose(ask_depth, required_depth):
        reason = "insufficient_ask_depth"
    return LiquidityAssessment(
        allowed=reason == "sufficient_depth",
        reason=reason,
        spread_pct=spread,
        bid_depth_usdt=bid_depth,
        ask_depth_usdt=ask_depth,
        required_side_depth_usdt=required_depth,
        book_timestamp_ns=book.ts,
    )

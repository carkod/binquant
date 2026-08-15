from collections import defaultdict, deque
from time import time

from market_regime.models import LiquidationEvent, LiquidationWindow


def canonical_derivatives_symbol(symbol: str) -> str:
    normalized = symbol.upper().strip().replace("-", "").replace("_", "")
    if normalized.startswith("XBT"):
        normalized = f"BTC{normalized[3:]}"
    for suffix in ("USDTM", "USDCM", "USDM", "USDTPERP", "USDCPERP", "USDPERP"):
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    for suffix in ("USDT", "USDC", "USD"):
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


class LiquidationStateStore:
    """Rolling public liquidation events, grouped by canonical base asset."""

    RETENTION_MS = 60 * 60 * 1000

    def __init__(self) -> None:
        self._events: dict[str, deque[LiquidationEvent]] = defaultdict(deque)

    def add(
        self,
        *,
        timestamp: int,
        symbol: str,
        order_side: str,
        price: float,
        quantity: float,
    ) -> None:
        if price <= 0 or quantity <= 0:
            return
        side = order_side.upper()
        if side not in {"BUY", "SELL"}:
            return

        # A forced SELL closes a long; a forced BUY closes a short.
        position_side = "LONG" if side == "SELL" else "SHORT"
        canonical_symbol = canonical_derivatives_symbol(symbol)
        events = self._events[canonical_symbol]
        events.append(
            LiquidationEvent(
                timestamp=timestamp,
                symbol=canonical_symbol,
                position_side=position_side,
                notional=price * quantity,
            )
        )
        self._prune(events, timestamp)

    def window(
        self,
        symbol: str,
        *,
        window_ms: int,
        as_of_ms: int | None = None,
    ) -> LiquidationWindow:
        as_of = as_of_ms if as_of_ms is not None else int(time() * 1000)
        events = self._events[canonical_derivatives_symbol(symbol)]
        self._prune(events, as_of)
        cutoff = as_of - window_ms
        long_notional = sum(
            event.notional
            for event in events
            if cutoff <= event.timestamp <= as_of and event.position_side == "LONG"
        )
        short_notional = sum(
            event.notional
            for event in events
            if cutoff <= event.timestamp <= as_of and event.position_side == "SHORT"
        )
        return LiquidationWindow(
            long_notional=long_notional,
            short_notional=short_notional,
        )

    def _prune(self, events: deque[LiquidationEvent], as_of_ms: int) -> None:
        cutoff = as_of_ms - self.RETENTION_MS
        while events and events[0].timestamp < cutoff:
            events.popleft()

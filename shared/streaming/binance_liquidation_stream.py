import asyncio
import json
import logging
from typing import Any

from websockets.asyncio.client import connect

from market_regime.liquidation_state_store import LiquidationStateStore


class BinanceLiquidationStream:
    """Collect Binance USD-M force-order events as a cross-exchange proxy."""

    URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

    def __init__(self, store: LiquidationStateStore) -> None:
        self.store = store

    def process_message(self, raw: str | bytes) -> None:
        decoded: Any = json.loads(raw)
        if not isinstance(decoded, dict):
            return
        payload: dict[str, Any] = decoded
        order = payload.get("o")
        if not isinstance(order, dict):
            return

        price = float(order.get("ap") or order.get("p") or 0)
        # Binance's `z` is cumulative filled quantity. The same forced order
        # can be reported more than once as it fills, so summing `z` inflates
        # liquidation notional. `l` is the incremental fill in this update.
        raw_quantity = order.get("l")
        if raw_quantity is None:
            raw_quantity = order.get("z") or order.get("q") or 0
        quantity = float(raw_quantity)
        timestamp = int(order.get("T") or payload.get("E") or 0)
        symbol = str(order.get("s") or "")
        side = str(order.get("S") or "")
        if timestamp <= 0 or not symbol:
            return
        self.store.add(
            timestamp=timestamp,
            symbol=symbol,
            order_side=side,
            price=price,
            quantity=quantity,
        )

    async def run_forever(self) -> None:
        backoff_seconds = 1
        while True:
            try:
                async with connect(
                    self.URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as websocket:
                    backoff_seconds = 1
                    async for raw in websocket:
                        try:
                            self.process_message(raw)
                        except (TypeError, ValueError, json.JSONDecodeError):
                            logging.exception("Invalid Binance liquidation event")
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception(
                    "Binance liquidation stream disconnected; reconnecting"
                )
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

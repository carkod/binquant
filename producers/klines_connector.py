import asyncio
import json
import logging
import random
from collections.abc import Callable

from producers.produce_klines import KlinesProducer
from shared.apis.binbot_api import BinbotApi
from shared.enums import BinanceKlineIntervals
from shared.exceptions import WebSocketError
from shared.streaming.async_producer import AsyncProducer
from shared.streaming.socket_client import AsyncSpotWebsocketStreamClient


class KlinesConnector(BinbotApi):
    """Async Klines connector using `AsyncSpotWebsocketStreamClient`.

    Splits symbols across multiple websocket clients (chunks of MAX_MARKETS_PER_CLIENT)
    and manages subscriptions & reconnections asynchronously.
    """

    MAX_MARKETS_PER_CLIENT = 200  # reduced to lessen payload and connection pressure

    def __init__(
        self,
        interval: BinanceKlineIntervals = BinanceKlineIntervals.five_minutes,
    ) -> None:
        logging.debug("Started Kafka producer SignalsInbound")
        super().__init__()
        self.interval = interval
        self.autotrade_settings = self.get_autotrade_settings()
        self.clients: list[AsyncSpotWebsocketStreamClient] = []
        self._client_markets: list[list[str]] = []  # store markets per client for resubscription
        # Per-client reconnect locks so one failing client doesn't block others
        self._reconnect_locks: list[asyncio.Lock] = []
        self._global_init_lock = asyncio.Lock()

    def _make_client(
        self,
        on_message: Callable,
        on_close: Callable | None = None,
        on_error: Callable | None = None,
    ) -> AsyncSpotWebsocketStreamClient:
        return AsyncSpotWebsocketStreamClient(
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
            auto_reconnect=True,
            ping_interval=30,
            ping_timeout=10,
        )

    async def handle_close(self, idx, _message):
        client = self.clients[idx]
        logging.info("Close event for client %s: code=%s reason=%s", idx, getattr(client._websocket, "close_code", None), getattr(client._websocket, "close_reason", None))
        await self._reconnect(idx)


    async def handle_error(self, idx, _socket, message):
        logging.error(f"WebSocket error (client {idx}): {message}")
        await self._reconnect(idx)
        raise WebSocketError(message)

    async def on_message(self, idx, _ws, message):
        try:
            res = json.loads(message)
        except Exception as e:
            logging.error(
                f"Failed to decode message (client {idx}): {e}, raw: {message}"
            )
            return
        if "e" in res and res["e"] == "kline":
            if res.get("k", {}).get("x"):
                logging.debug(f"Closed kline (client {idx}): {res}")
            await self.process_kline_stream(res)
        else:
            logging.debug(f"Non-kline event received (client {idx}): {res}")

    async def start_stream(self) -> None:
        """
        Kline/Candlestick Streams

        The Kline/Candlestick Stream push updates to the current klines/candlestick every second.
        Stream Name: <symbol>@kline_<interval>
        Check BinanceKlineIntervals Enum for possible values
        Update Speed: 2000ms

        Split symbols into chunks of MAX_MARKETS_PER_CLIENT
        """
        # Initialize low-latency Kafka producer (returns started producer instance)
        self.producer = await AsyncProducer.initialize()
        symbols = self.get_symbols()
        symbol_chunks = [
            symbols[i : i + self.MAX_MARKETS_PER_CLIENT]
            for i in range(0, len(symbols), self.MAX_MARKETS_PER_CLIENT)
        ]
        symbol_chunks = symbol_chunks[:5]  # LIMIT TO FIRST 5 CHUNKS FOR TESTING
        for idx, chunk in enumerate(symbol_chunks):
            markets = [
                f"{symbol['id'].lower()}@kline_{self.interval.value}"
                for symbol in chunk
            ]
            client = self._make_client(
                lambda c, raw, i=idx: asyncio.create_task(self.on_message(i, c, raw)),
                lambda c, i=idx: asyncio.create_task(self.handle_close(i, None)),
                lambda c, err, i=idx: asyncio.create_task(self.handle_error(i, c, err)),
            )
            self.clients.append(client)
            self._client_markets.append(markets)
            self._reconnect_locks.append(asyncio.Lock())
            # Connect first, then ramp subscriptions.
            await client.connect()
            # Initial tiny payload: first 3 streams only.
            initial = markets[:3]
            await client.subscribe(initial)
            remaining = markets[3:]
            # Ramp remaining in chunks of 50 with a short delay to avoid large first frame.
            for m_chunk in [remaining[i:i+50] for i in range(0, len(remaining), 50)]:
                if not m_chunk:
                    break
                await asyncio.sleep(0.15)  # small pause between batches
                await client.subscribe(m_chunk)

    async def process_kline_stream(self, result):
        """
        Updates market data in DB for research
        """
        symbol = result["k"]["s"]
        # Offload storing so we never block the websocket message loop.
        if symbol and "k" in result and result["k"]["x"]:
            print(f"kline processed for {symbol}")

    async def _store_async(self, symbol: str, result):
        try:
            klines_producer = KlinesProducer(self.producer, symbol)
            await klines_producer.store(result)
        except Exception as e:
            logging.error(f"Store failed for {symbol}: {e}")


    async def _reconnect(self, idx: int) -> None:
        if idx >= len(self.clients):
            logging.warning(f"_reconnect: invalid client index {idx}")
            return
        lock = self._reconnect_locks[idx]
        if lock.locked():
            logging.debug(f"_reconnect: client {idx} already reconnecting")
            return
        markets = self._client_markets[idx]
        async with lock:
            delay = random.uniform(0.5, 2.0)
            logging.info(f"Reconnecting client {idx} after {delay:.2f}s; markets={len(markets)}")
            await asyncio.sleep(delay)
            client = self.clients[idx]
            try:
                await client.connect()
            except Exception as e:
                logging.error(f"Reconnect connect() failed for client {idx}: {e}")
                return
            try:
                # Reconnect sequence: start with first 3 then ramp.
                initial = markets[:3]
                await client.subscribe(initial)
                remaining = markets[3:]
                for m_chunk in [remaining[i:i+50] for i in range(0, len(remaining), 50)]:
                    if not m_chunk:
                        break
                    await asyncio.sleep(0.15)
                    await client.subscribe(m_chunk)
            except Exception as e:
                logging.error(f"Reconnect subscribe() failed for client {idx}: {e}")
                return
            logging.info(f"Client {idx} reconnected and re-subscribed ({len(markets)} streams)")

    async def run(self) -> None:
        await self.start_stream()
        backoffs = [1.0 for _ in self.clients]
        while True:
            for idx, client in enumerate(self.clients):
                if not client.is_connected:
                    logging.warning(f"Watchdog: client {idx} disconnected; attempting reconnect")
                    try:
                        await self._reconnect(idx)
                        backoffs[idx] = 1.0
                    except Exception as e:
                        logging.error(f"Watchdog: reconnect failed for client {idx}: {e}; backoff={backoffs[idx]:.1f}s")
                        await asyncio.sleep(backoffs[idx])
                        backoffs[idx] = min(backoffs[idx] * 2, 60.0)
                else:
                    try:
                        await client.list_subscriptions()
                    except Exception as e:
                        logging.debug(f"Watchdog: list_subscriptions failed for client {idx}: {e}")
            await asyncio.sleep(30)

    # Removed manual keepalive; relying on built-in ping_interval in client.

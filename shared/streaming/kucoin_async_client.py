import asyncio
import logging
import os
from collections.abc import Coroutine
from concurrent.futures import Future as ConcurrencyFuture
from typing import Any, cast

from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.generate.spot.spot_public.model_klines_event import (
    KlinesEvent,
)
from kucoin_universal_sdk.model.client_option import ClientOptionBuilder
from kucoin_universal_sdk.model.constants import (
    GLOBAL_API_ENDPOINT,
    GLOBAL_BROKER_API_ENDPOINT,
    GLOBAL_FUTURES_API_ENDPOINT,
)
from kucoin_universal_sdk.model.transport_option import TransportOptionBuilder
from kucoin_universal_sdk.model.websocket_option import WebSocketClientOptionBuilder

from models.klines import KlineProduceModel
from shared.enums import KafkaTopics
from shared.streaming.async_producer import AsyncProducer

logger = logging.getLogger(__name__)


class AsyncKucoinWebsocketClient:
    """
    Async KuCoin WebSocket client.
    Subscriptions are queued and flushed ONLY after WELCOME frame.
    """

    def __init__(self, producer: AsyncProducer):
        self.api_key = os.getenv("KUCOIN_API_KEY", "")
        self.api_secret = os.getenv("KUCOIN_API_SECRET", "")
        self.api_passphrase = os.getenv("KUCOIN_API_PASSPHRASE", "")

        transport_option = (
            TransportOptionBuilder()
            .set_interceptors([])
            .set_read_timeout(30)
            .set_keep_alive(True)
            .build()
        )

        websocket_option = (
            WebSocketClientOptionBuilder()
            .with_reconnect(self._on_thread_reconnect)
            .with_reconnect_attempts(-1)
            .with_reconnect_interval(5)
            .build()
        )

        client_option = (
            ClientOptionBuilder()
            .set_key(self.api_key)
            .set_secret(self.api_secret)
            .set_passphrase(self.api_passphrase)
            .set_spot_endpoint(GLOBAL_API_ENDPOINT)
            .set_futures_endpoint(GLOBAL_FUTURES_API_ENDPOINT)
            .set_broker_endpoint(GLOBAL_BROKER_API_ENDPOINT)
            .set_transport_option(transport_option)
            .set_websocket_client_option(websocket_option)
            .build()
        )

        self._client = DefaultClient(client_option)
        self.ws_service = self._client.ws_service()
        self.spot_public_ws = self.ws_service.new_spot_public_ws()

        # Async state
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws_future: ConcurrencyFuture[Any] | None = None

        self._connected_event = asyncio.Event()  # TCP open
        self._ready_event = asyncio.Event()  # WELCOME received

        # Subscription tracking
        self.subscriptions: dict[str, str] = {}
        self._pending_klines: list[tuple[str, str]] = []

        self.producer = producer

    # ------------------------------------------------------------------
    # WebSocket lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        if self._ws_future:
            return

        self._loop = asyncio.get_running_loop()
        self._connected_event.clear()
        self._ready_event.clear()

        logger.info("Starting KuCoin websocket…")

        self._ws_future = cast(
            ConcurrencyFuture[Any],
            self._loop.run_in_executor(None, self.spot_public_ws.start),
        )

        await self._connected_event.wait()
        logger.info("KuCoin TCP connection established")

    async def run_forever(self):
        while True:
            await self.start()

            try:
                if self._ws_future:
                    await asyncio.wrap_future(self._ws_future)
            except Exception:
                logger.exception("KuCoin WS crashed")
            finally:
                self._ws_future = None
                self._connected_event.clear()
                self._ready_event.clear()
                await asyncio.sleep(3)

    # ------------------------------------------------------------------
    # Connection events
    # ------------------------------------------------------------------

    async def on_open(self, ws=None):
        logger.info("WebSocket connection opened")
        self._connected_event.set()

    async def on_close(self, ws=None):
        logger.warning("WebSocket closed")

    async def _on_thread_reconnect(self):
        if self._loop:
            asyncio.run_coroutine_threadsafe(self._handle_reconnect(), self._loop)

    async def _handle_reconnect(self):
        logger.warning("Reconnect requested")
        self._ready_event.clear()
        await self.start()

    # ------------------------------------------------------------------
    # Scheduling helper
    # ------------------------------------------------------------------

    def _schedule(self, coro: Coroutine[Any, Any, Any]) -> None:
        if not self._loop:
            return

        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)

        def _done(f: ConcurrencyFuture):
            try:
                f.result()
            except Exception:
                logger.exception("Scheduled task failed")

        fut.add_done_callback(_done)

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    def _callback(self, topic: str, subject: str, event: Any):
        # WELCOME detection (CRITICAL)
        if isinstance(event, dict) and event.get("type") == "welcome":
            logger.info("WELCOME received — WS is ready")
            self._ready_event.set()
            self._schedule(self._flush_pending_klines())
            return

        if subject == "trade.candles.update":
            self._schedule(self.process_kline_stream(event))

    async def process_kline_stream(self, event: KlinesEvent):
        candles = event.candles
        if not candles or len(candles) < 6:
            return

        ts = int(candles[0])

        kline = KlineProduceModel(
            symbol=event.symbol,
            open_time=str(ts * 1000),
            close_time=str((ts + 60) * 1000),
            open_price=str(candles[1]),
            close_price=str(candles[2]),
            high_price=str(candles[3]),
            low_price=str(candles[4]),
            volume=str(candles[5]),
        )

        await self.producer.send(
            topic=KafkaTopics.klines_store_topic.value,
            value=kline.model_dump_json(),
            key=event.symbol,
            timestamp=ts * 1000,
        )

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    async def subscribe_klines(self, symbol: str, interval: str):
        self._pending_klines.append((symbol, interval))
        logger.info(f"Queued klines: {symbol} {interval}")

        if self._ready_event.is_set():
            await self._flush_pending_klines()

    async def _flush_pending_klines(self):
        if not self._ready_event.is_set():
            return

        for symbol, interval in list(self._pending_klines):
            try:
                sub_id = await asyncio.get_running_loop().run_in_executor(
                    None,
                    self.spot_public_ws.klines,
                    symbol,
                    interval,
                    self._callback,
                )
                self.subscriptions[f"{symbol}:{interval}"] = sub_id
                self._pending_klines.remove((symbol, interval))
                logger.info(f"Subscribed: {symbol} {interval}")

            except Exception:
                logger.warning(
                    f"Subscribe failed (retry on reconnect): {symbol} {interval}"
                )

    async def unsubscribe(self, key: str):
        sub_id = self.subscriptions.pop(key, None)
        if not sub_id:
            return

        await asyncio.get_running_loop().run_in_executor(
            None, self.spot_public_ws.unsubscribe, sub_id
        )

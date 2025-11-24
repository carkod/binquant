import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
from aiohttp import ClientWebSocketResponse, WSMsgType

logger = logging.getLogger(__name__)

CallbackType = Callable[..., Any]
AsyncCallbackType = Callable[..., Awaitable[Any]]


class AsyncKucoinWebsocketClient:
    """Async Kucoin WebSocket client.

    Mirrors the public API of AsyncBinanceWebsocketClient but adapted for Kucoin's
    WebSocket protocol which uses topic-based subscriptions and different message formats.

    Lifecycle:
        client = AsyncKucoinWebsocketClient(...)
        await client.start()
        await client.subscribe(["/market/ticker:BTC-USDT"])
        ...
        await client.stop()

    Notes:
        - Callbacks may be sync or async; async callbacks are awaited.
        - Automatic reconnect is supported (disabled by default). When enabled
          subscriptions are re-sent after successful reconnect.
        - Kucoin uses topic-based subscriptions (e.g., "/market/candles:BTC-USDT_1min")
        - Ping/Pong frames are surfaced to provided callbacks if present.
    """

    ACTION_SUBSCRIBE = "subscribe"
    ACTION_UNSUBSCRIBE = "unsubscribe"

    def __init__(
        self,
        stream_url: str = "wss://ws-api-spot.kucoin.com/",
        on_message: CallbackType | AsyncCallbackType | None = None,
        on_open: CallbackType | AsyncCallbackType | None = None,
        on_close: CallbackType | AsyncCallbackType | None = None,
        on_error: CallbackType | AsyncCallbackType | None = None,
        on_ping: CallbackType | AsyncCallbackType | None = None,
        on_pong: CallbackType | AsyncCallbackType | None = None,
        reconnect: bool = True,
        max_retries: int = 12,
        backoff_base: float = 0.75,
        heartbeat_interval: int = 30,
    ) -> None:
        self._stream_url = stream_url
        self._session: aiohttp.ClientSession | None = None
        self._ws: ClientWebSocketResponse | None = None
        self._read_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._stopped = False
        self._subscriptions: set[str] = set()
        self._reconnect_enabled = reconnect
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._heartbeat_interval = heartbeat_interval

        self.on_message = on_message
        self.on_open = on_open
        self.on_close = on_close
        self.on_error = on_error
        self.on_ping = on_ping
        self.on_pong = on_pong

    async def start(self) -> None:
        """Establish the websocket connection and start the read loop."""
        if self._ws and not self._ws.closed:
            return
        self._session = aiohttp.ClientSession()
        await self._connect_and_start_read_loop()

    async def _connect_and_start_read_loop(self) -> None:
        retry = 0
        while True:
            try:
                logger.debug(
                    "Connecting to Kucoin WebSocket: %s (retry=%s)",
                    self._stream_url,
                    retry,
                )
                assert self._session is not None
                self._ws = await self._session.ws_connect(
                    self._stream_url,
                    heartbeat=self._heartbeat_interval,
                    compress=0,
                    timeout=aiohttp.ClientWSTimeout(ws_close=30),
                )
                await self._dispatch(self.on_open)
                if retry > 0:
                    logger.info("Reconnected successfully after %s retries", retry)
                if self._subscriptions:
                    await self.subscribe(list(self._subscriptions))
                self._read_task = asyncio.create_task(self._read_loop())
                self._ping_task = asyncio.create_task(self._ping_loop())
                return
            except Exception as e:
                await self._dispatch(self.on_error, e)
                logger.error("WebSocket connect failed: %s", e, exc_info=True)
                if not self._reconnect_enabled or retry >= self._max_retries:
                    raise
                delay = self._backoff_base * (2**retry)
                jitter = random.uniform(0, self._backoff_base)
                wait_for = min(delay + jitter, 60)
                logger.warning("Retrying websocket connection in %.2fs", wait_for)
                await asyncio.sleep(wait_for)
                retry += 1

    async def _ping_loop(self) -> None:
        """Send periodic ping messages to keep connection alive.

        Kucoin requires periodic pings to maintain the connection.
        """
        while not self._stopped and self._ws and not self._ws.closed:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                await self.ping()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in ping loop: %s", e)

    async def _read_loop(self) -> None:
        """
        Continuously consume websocket messages until stopped or closed.
        """
        assert self._ws is not None
        ws = self._ws
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    await self._dispatch(self.on_message, msg.data)
                elif msg.type == WSMsgType.BINARY:
                    await self._dispatch(self.on_message, msg.data)
                elif msg.type == WSMsgType.PING:
                    logger.debug("Received PING frame")
                    await self._dispatch(self.on_ping, msg.data)
                elif msg.type == WSMsgType.PONG:
                    logger.debug("Received PONG frame")
                    await self._dispatch(self.on_pong, msg.data)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                    logger.warning(
                        "WebSocket closing (type=%s, code=%s, message=%s)",
                        msg.type,
                        ws.close_code,
                        getattr(ws, "close_message", None),
                    )
                    await self._dispatch(self.on_close, ws.close_code)
                    break
                elif msg.type == WSMsgType.ERROR:
                    err = ws.exception()
                    logger.error("WebSocket error frame: %s", err)
                    await self._dispatch(self.on_error, err)
                    break
        except Exception as e:
            await self._dispatch(self.on_error, e)
            logger.error("Exception in read loop: %s", e, exc_info=True)
        finally:
            if not self._stopped and self._reconnect_enabled:
                logger.warning("WebSocket disconnected; attempting reconnect...")
                try:
                    await self._connect_and_start_read_loop()
                except Exception as e:
                    logger.error("Reconnect failed: %s", e, exc_info=True)
                    await self.stop()

    async def run_forever(self) -> None:
        """Block until the websocket stops. Useful replacement for thread.join()."""
        stop_event = asyncio.Event()

        async def on_close_wrapper(client, *args):
            try:
                if self.on_close and self.on_close is not on_close_wrapper:
                    if asyncio.iscoroutinefunction(self.on_close):
                        await self.on_close(client, *args)
                    else:
                        self.on_close(client, *args)
            finally:
                stop_event.set()

        original_on_close = self.on_close
        self.on_close = on_close_wrapper
        try:
            await stop_event.wait()
        finally:
            self.on_close = original_on_close

    async def _dispatch(
        self, callback: CallbackType | AsyncCallbackType | None, *args
    ) -> None:
        if not callback:
            return
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(self, *args)
            else:
                callback(self, *args)
        except Exception as e:
            logger.error("Callback error: %s", e, exc_info=True)
            if callback is not self.on_error and self.on_error:
                try:
                    if asyncio.iscoroutinefunction(self.on_error):
                        await self.on_error(self, e)
                    else:
                        self.on_error(self, e)
                except Exception:
                    logger.error("Error handler raised", exc_info=True)

    def get_timestamp(self) -> int:
        return int(time.time() * 1000)

    async def send(self, message: dict) -> None:
        if not self._ws or self._ws.closed:
            raise RuntimeError("WebSocket not connected")
        data = json.dumps(message)
        await self._ws.send_str(data)
        logger.debug("Sent message: %s", data)

    async def send_message_to_server(
        self, message, action: str | None = None, id: int | None = None
    ) -> None:
        if not id:
            id = self.get_timestamp()
        if action != self.ACTION_UNSUBSCRIBE:
            await self.subscribe(message, id=id)
        else:
            await self.unsubscribe(message, id=id)

    async def subscribe(self, topic: str | list[str], id: int | None = None) -> None:
        """Subscribe to Kucoin topic(s)

        Kucoin format: {"id": "1", "type": "subscribe", "topic": "/market/ticker:BTC-USDT", "response": true}
        """
        if not id:
            id = self.get_timestamp()
        if isinstance(topic, str):
            topics = [topic]
        elif isinstance(topic, list):
            topics = topic
        else:
            raise ValueError("Invalid topic, expect string or list")

        # Track subscriptions for reconnect
        for t in topics:
            self._subscriptions.add(t)
            await self.send(
                {"id": str(id), "type": "subscribe", "topic": t, "response": True}
            )

    async def unsubscribe(self, topic: str, id: int | None = None) -> None:
        """Unsubscribe from Kucoin topic

        Kucoin format: {"id": "1", "type": "unsubscribe", "topic": "/market/ticker:BTC-USDT", "response": true}
        """
        if not id:
            id = self.get_timestamp()
        if not isinstance(topic, str):
            raise ValueError("Invalid topic name, expect a string")
        self._subscriptions.discard(topic)
        await self.send(
            {"id": str(id), "type": "unsubscribe", "topic": topic, "response": True}
        )

    async def ping(self) -> None:
        """Send ping to Kucoin WebSocket Server

        Kucoin uses a different ping format: {"id": "1", "type": "ping"}
        """
        if not self._ws or self._ws.closed:
            raise RuntimeError("WebSocket not connected")
        id = self.get_timestamp()
        await self.send({"id": str(id), "type": "ping"})
        logger.debug("Ping message sent")

    async def stop(self) -> None:
        self._stopped = True
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
            self._read_task = None
        if self._ws and not self._ws.closed:
            await self._ws.close()
            await self._dispatch(self.on_close, "closed")
        if self._session:
            await self._session.close()
        self._ws = None
        self._session = None
        logger.info("Async Kucoin WebSocket client stopped")


class AsyncKucoinSpotWebsocketStreamClient(AsyncKucoinWebsocketClient):
    """Convenience wrapper for Kucoin spot market streams."""

    def __init__(
        self,
        stream_url: str = "wss://ws-api-spot.kucoin.com/",
        on_message: CallbackType | AsyncCallbackType | None = None,
        on_open: CallbackType | AsyncCallbackType | None = None,
        on_close: CallbackType | AsyncCallbackType | None = None,
        on_error: CallbackType | AsyncCallbackType | None = None,
        on_ping: CallbackType | AsyncCallbackType | None = None,
        on_pong: CallbackType | AsyncCallbackType | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            stream_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_ping=on_ping,
            on_pong=on_pong,
            **kwargs,
        )

    async def klines(
        self,
        markets: list[str],
        interval: str,
        id: int | None = None,
        action: str | None = None,
    ) -> None:
        """Convenience method to subscribe/unsubscribe kline streams.

        Kucoin kline topic format: /market/candles:{symbol}_{interval}
        Example: /market/candles:BTC-USDT_1min

        If markets is empty, subscribes to a dummy stream to keep connection active.
        """
        topics: list[str] = []
        if len(markets) == 0:
            markets.append("BTC-USDT")
        for market in markets:
            # Kucoin uses different format: symbol is already formatted like BTC-USDT
            topics.append(f"/market/candles:{market.upper()}_{interval}")
        await self.send_message_to_server(topics, action=action, id=id)

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


class AsyncBinanceWebsocketClient:
    """Async Binance WebSocket client.

    Mirrors the public API of the synchronous `BinanceWebsocketClient` but uses
    asyncio + aiohttp under the hood for non-blocking IO.

    Lifecycle:
        client = AsyncBinanceWebsocketClient(...)
        await client.start()
        await client.subscribe(["btcusdc@kline_1m"])  # etc
        ...
        await client.stop()

    Notes:
        - Callbacks may be sync or async; async callbacks are awaited.
        - Automatic reconnect is supported (disabled by default). When enabled
          subscriptions are re-sent after successful reconnect.
        - Ping/Pong frames are surfaced to provided callbacks if present.
    """

    ACTION_SUBSCRIBE = "SUBSCRIBE"
    ACTION_UNSUBSCRIBE = "UNSUBSCRIBE"

    def __init__(
        self,
        stream_url: str = "wss://stream.binance.com:443/ws",
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
                    "Connecting to Binance WebSocket: %s (retry=%s)",
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

    async def subscribe(self, stream: str | list[str], id: int | None = None) -> None:
        if not id:
            id = self.get_timestamp()
        if isinstance(stream, str):
            params = [stream]
        elif isinstance(stream, list):
            params = stream
        else:
            raise ValueError("Invalid stream, expect string or list")
        # Track subscriptions for reconnect
        for s in params:
            self._subscriptions.add(s)
        await self.send({"method": "SUBSCRIBE", "params": params, "id": id})

    async def unsubscribe(self, stream: str, id: int | None = None) -> None:
        if not id:
            id = self.get_timestamp()
        if not isinstance(stream, str):
            raise ValueError("Invalid stream name, expect a string")
        self._subscriptions.discard(stream)
        await self.send({"method": "UNSUBSCRIBE", "params": [stream], "id": id})

    async def list_subscribe(self, id: int | None = None) -> None:
        if not id:
            id = self.get_timestamp()
        await self.send({"method": "LIST_SUBSCRIPTIONS", "id": id})

    async def ping(self) -> None:
        if not self._ws or self._ws.closed:
            raise RuntimeError("WebSocket not connected")
        await self._ws.ping()
        logger.debug("Ping frame sent")

    async def stop(self) -> None:
        self._stopped = True
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
        logger.info("Async Binance WebSocket client stopped")


class AsyncSpotWebsocketStreamClient(AsyncBinanceWebsocketClient):
    """Convenience wrapper for spot market streams (combined or single)."""

    def __init__(
        self,
        stream_url: str = "wss://stream.binance.com:443",
        on_message: CallbackType | AsyncCallbackType | None = None,
        on_open: CallbackType | AsyncCallbackType | None = None,
        on_close: CallbackType | AsyncCallbackType | None = None,
        on_error: CallbackType | AsyncCallbackType | None = None,
        on_ping: CallbackType | AsyncCallbackType | None = None,
        on_pong: CallbackType | AsyncCallbackType | None = None,
        is_combined: bool = False,
        **kwargs,
    ) -> None:
        if is_combined:
            stream_url = stream_url + "/stream"
        else:
            stream_url = stream_url + "/ws"
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

        If markets is empty, subscribes to a dummy stream to keep connection active.
        """
        params: list[str] = []
        if len(markets) == 0:
            markets.append("BNBBTC")
        for market in markets:
            params.append(f"{market.lower()}@kline_{interval}")
        await self.send_message_to_server(params, action=action, id=id)

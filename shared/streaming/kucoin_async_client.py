import asyncio
import logging
from collections.abc import Callable
from typing import Any

from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.generate.spot.spot_public.ws_spot_public import SpotPublicWS
from kucoin_universal_sdk.model.client_option import ClientOptionBuilder
from kucoin_universal_sdk.model.constants import (
    GLOBAL_API_ENDPOINT,
    GLOBAL_BROKER_API_ENDPOINT,
    GLOBAL_FUTURES_API_ENDPOINT,
)
from kucoin_universal_sdk.model.transport_option import TransportOptionBuilder
from kucoin_universal_sdk.model.websocket_option import WebSocketClientOptionBuilder

logger = logging.getLogger(__name__)


class AsyncKucoinWebsocketClient:
    """
    Async wrapper around KuCoin Universal SDK's WebSocket client.

    Provides an async interface compatible with the existing async Binance
    websocket implementation pattern.
    """

    ACTION_SUBSCRIBE = "subscribe"
    ACTION_UNSUBSCRIBE = "unsubscribe"

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        on_message: Callable | None = None,
        on_open: Callable | None = None,
        on_close: Callable | None = None,
        on_error: Callable | None = None,
        reconnect: bool = True,
    ):
        """
        Initialize async Kucoin WebSocket client using the official SDK.

        Args:
            api_key: KuCoin API key (optional for public channels)
            api_secret: KuCoin API secret (optional for public channels)
            api_passphrase: KuCoin API passphrase (optional for public channels)
            on_message: Async callback for messages
            on_open: Async callback for connection open
            on_close: Async callback for connection close
            on_error: Async callback for errors
            reconnect: Enable automatic reconnection (SDK handles this internally)
        """
        # Build client options
        transport_option = (
            TransportOptionBuilder()
            .set_interceptors([])
            .set_read_timeout(30)
            .set_keep_alive(True)
            .build()
        )

        websocket_option = (
            WebSocketClientOptionBuilder()
            .with_reconnect(reconnect)
            .with_reconnect_attempts(-1 if reconnect else 0)
            .with_reconnect_interval(5)
            .build()
        )

        client_option = (
            ClientOptionBuilder()
            .set_key(api_key)
            .set_secret(api_secret)
            .set_passphrase(api_passphrase)
            .set_spot_endpoint(GLOBAL_API_ENDPOINT)
            .set_futures_endpoint(GLOBAL_FUTURES_API_ENDPOINT)
            .set_broker_endpoint(GLOBAL_BROKER_API_ENDPOINT)
            .set_transport_option(transport_option)
            .set_websocket_client_option(websocket_option)
            .build()
        )

        # Create default client
        self._client = DefaultClient(client_option)
        self._ws_service = self._client.ws_service()
        self._spot_public_ws: SpotPublicWS | None = None

        # Store callbacks
        self.on_message = on_message
        self.on_open = on_open
        self.on_close = on_close
        self.on_error = on_error
        self._reconnect = reconnect

        # Track subscriptions
        self._subscriptions: dict[str, str] = {}
        self._started = False

    async def start(self):
        """Start the WebSocket connection."""
        if not self._started:
            # Run SDK start in thread pool since it's synchronous
            await asyncio.get_event_loop().run_in_executor(None, self._start_sync)
            if self.on_open:
                if asyncio.iscoroutinefunction(self.on_open):
                    await self.on_open(self)
                else:
                    self.on_open(self)
            logger.info("Async KuCoin WebSocket client started")

    def _start_sync(self):
        """Synchronous start helper."""
        self._spot_public_ws = self._ws_service.new_spot_public_ws()
        self._spot_public_ws.start()
        self._started = True

    async def stop(self):
        """Stop the WebSocket connection."""
        if self._spot_public_ws:
            await asyncio.get_event_loop().run_in_executor(
                None, self._spot_public_ws.stop
            )
            self._started = False
            if self.on_close:
                if asyncio.iscoroutinefunction(self.on_close):
                    await self.on_close(self)
                else:
                    self.on_close(self)
            logger.info("Async KuCoin WebSocket client stopped")

    async def subscribe(self, topic: str | list[str], id: int | None = None):
        """
        Subscribe to topic(s).

        Args:
            topic: Topic string or list of topics
            id: Optional subscription ID (not used by SDK)
        """
        topics = [topic] if isinstance(topic, str) else topic

        for t in topics:
            # Parse topic to determine subscription type
            # Expected formats: "/market/candles:BTC-USDT_1min" or similar
            if "/market/candles:" in t:
                parts = t.split(":")
                if len(parts) == 2:
                    symbol_interval = parts[1]
                    symbol, interval = symbol_interval.rsplit("_", 1)
                    await self.subscribe_klines(symbol, interval)
            elif "/market/ticker:" in t:
                symbol = t.split(":")[1] if ":" in t else t
                await self.subscribe_ticker([symbol])

    async def subscribe_klines(self, symbol: str, interval: str):
        """
        Subscribe to klines (candlestick) data.

        Args:
            symbol: Trading pair (e.g., "BTC-USDT")
            interval: Interval string (e.g., "1min", "5min", "1hour")
        """
        if not self._spot_public_ws:
            raise RuntimeError("WebSocket not started. Call start() first.")

        def callback(topic: str, subject: str, event: Any):
            """Internal callback that wraps the user's async callback."""
            if self.on_message:
                message_data = {
                    "type": "message",
                    "topic": topic,
                    "subject": subject,
                    "data": event.__dict__ if hasattr(event, "__dict__") else event,
                }
                # Schedule the async callback
                if asyncio.iscoroutinefunction(self.on_message):
                    asyncio.create_task(self.on_message(self, message_data))
                else:
                    self.on_message(self, message_data)

        try:
            sub_id = await asyncio.get_event_loop().run_in_executor(
                None, self._spot_public_ws.klines, symbol, interval, callback
            )
            self._subscriptions[f"{symbol}:{interval}"] = sub_id
            logger.info(f"Subscribed to klines: {symbol} @ {interval}")
        except Exception as e:
            logger.error(f"Error subscribing to klines: {e}")
            if self.on_error:
                if asyncio.iscoroutinefunction(self.on_error):
                    await self.on_error(self, e)
                else:
                    self.on_error(self, e)

    async def subscribe_ticker(self, symbols: list[str]):
        """
        Subscribe to ticker data for one or more symbols.

        Args:
            symbols: List of trading pairs (e.g., ["BTC-USDT", "ETH-USDT"])
        """
        if not self._spot_public_ws:
            raise RuntimeError("WebSocket not started. Call start() first.")

        def callback(topic: str, subject: str, event: Any):
            """Internal callback that wraps the user's async callback."""
            if self.on_message:
                message_data = {
                    "type": "message",
                    "topic": topic,
                    "subject": subject,
                    "data": event.__dict__ if hasattr(event, "__dict__") else event,
                }
                # Schedule the async callback
                if asyncio.iscoroutinefunction(self.on_message):
                    asyncio.create_task(self.on_message(self, message_data))
                else:
                    self.on_message(self, message_data)

        try:
            sub_id = await asyncio.get_event_loop().run_in_executor(
                None, self._spot_public_ws.ticker, symbols, callback
            )
            self._subscriptions[f"ticker:{','.join(symbols)}"] = sub_id
            logger.info(f"Subscribed to ticker: {symbols}")
        except Exception as e:
            logger.error(f"Error subscribing to ticker: {e}")
            if self.on_error:
                if asyncio.iscoroutinefunction(self.on_error):
                    await self.on_error(self, e)
                else:
                    self.on_error(self, e)

    async def unsubscribe(self, subscription_key: str, id: int | None = None):
        """
        Unsubscribe from a subscription.

        Args:
            subscription_key: The key used when subscribing
            id: Optional subscription ID (not used)
        """
        if not self._spot_public_ws:
            return

        if subscription_key in self._subscriptions:
            sub_id = self._subscriptions[subscription_key]
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, self._spot_public_ws.unsubscribe, sub_id
                )
                del self._subscriptions[subscription_key]
                logger.info(f"Unsubscribed from: {subscription_key}")
            except Exception as e:
                logger.error(f"Error unsubscribing: {e}")
                if self.on_error:
                    if asyncio.iscoroutinefunction(self.on_error):
                        await self.on_error(self, e)
                    else:
                        self.on_error(self, e)

    async def run_forever(self):
        """Keep the connection alive until stopped."""
        while self._started:
            await asyncio.sleep(1)


class AsyncKucoinSpotWebsocketStreamClient(AsyncKucoinWebsocketClient):
    """
    Async Kucoin spot WebSocket stream client.

    Provides convenience methods for spot market subscriptions.
    """

    async def klines(
        self,
        markets: list[str],
        interval: str,
        id: int | None = None,
        action: str | None = None,
    ):
        """
        Subscribe to klines for multiple markets.

        Args:
            markets: List of trading pairs (e.g., ["BTC-USDT", "ETH-USDT"])
            interval: Interval string (e.g., "1min", "5min", "1hour")
            id: Optional subscription ID
            action: Action (subscribe/unsubscribe)
        """
        if action == self.ACTION_UNSUBSCRIBE:
            for market in markets:
                await self.unsubscribe(f"{market}:{interval}")
        else:
            for market in markets:
                await self.subscribe_klines(market, interval)

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


class KucoinWebsocketClient:
    """
    Wrapper around KuCoin Universal SDK's WebSocket client.

    Provides a simplified interface compatible with the existing Binance
    websocket implementation pattern used in this project.
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
    ):
        """
        Initialize Kucoin WebSocket client using the official SDK.

        Args:
            api_key: KuCoin API key (optional for public channels)
            api_secret: KuCoin API secret (optional for public channels)
            api_passphrase: KuCoin API passphrase (optional for public channels)
            on_message: Callback for messages
            on_open: Callback for connection open
            on_close: Callback for connection close
            on_error: Callback for errors
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
            .with_reconnect(True)
            .with_reconnect_attempts(-1)
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

        # Track subscriptions
        self._subscriptions: dict[str, str] = {}
        self._started = False

    def start(self):
        """Start the WebSocket connection."""
        if not self._started:
            self._spot_public_ws = self._ws_service.new_spot_public_ws()
            self._spot_public_ws.start()
            self._started = True
            if self.on_open:
                self.on_open(self)
            logger.info("KuCoin WebSocket client started")

    def stop(self):
        """Stop the WebSocket connection."""
        if self._spot_public_ws:
            self._spot_public_ws.stop()
            self._started = False
            if self.on_close:
                self.on_close(self)
            logger.info("KuCoin WebSocket client stopped")

    def subscribe_klines(self, symbol: str, interval: str):
        """
        Subscribe to klines (candlestick) data.

        Args:
            symbol: Trading pair (e.g., "BTC-USDT")
            interval: Interval string (e.g., "1min", "5min", "1hour")
        """
        if not self._spot_public_ws:
            raise RuntimeError("WebSocket not started. Call start() first.")

        def callback(topic: str, subject: str, event: Any):
            """Internal callback that wraps the user's callback."""
            if self.on_message:
                # Convert SDK event to a format compatible with existing handlers
                message_data = {
                    "type": "message",
                    "topic": topic,
                    "subject": subject,
                    "data": event.__dict__ if hasattr(event, "__dict__") else event,
                }
                self.on_message(self, message_data)

        try:
            sub_id = self._spot_public_ws.klines(symbol, interval, callback)
            self._subscriptions[f"{symbol}:{interval}"] = sub_id
            logger.info(f"Subscribed to klines: {symbol} @ {interval}")
        except Exception as e:
            logger.error(f"Error subscribing to klines: {e}")
            if self.on_error:
                self.on_error(self, e)

    def subscribe_ticker(self, symbols: list[str]):
        """
        Subscribe to ticker data for one or more symbols.

        Args:
            symbols: List of trading pairs (e.g., ["BTC-USDT", "ETH-USDT"])
        """
        if not self._spot_public_ws:
            raise RuntimeError("WebSocket not started. Call start() first.")

        def callback(topic: str, subject: str, event: Any):
            """Internal callback that wraps the user's callback."""
            if self.on_message:
                message_data = {
                    "type": "message",
                    "topic": topic,
                    "subject": subject,
                    "data": event.__dict__ if hasattr(event, "__dict__") else event,
                }
                self.on_message(self, message_data)

        try:
            sub_id = self._spot_public_ws.ticker(symbols, callback)
            self._subscriptions[f"ticker:{','.join(symbols)}"] = sub_id
            logger.info(f"Subscribed to ticker: {symbols}")
        except Exception as e:
            logger.error(f"Error subscribing to ticker: {e}")
            if self.on_error:
                self.on_error(self, e)

    def unsubscribe(self, subscription_key: str):
        """
        Unsubscribe from a subscription.

        Args:
            subscription_key: The key used when subscribing (e.g., "BTC-USDT:1min")
        """
        if not self._spot_public_ws:
            return

        if subscription_key in self._subscriptions:
            sub_id = self._subscriptions[subscription_key]
            try:
                self._spot_public_ws.unsubscribe(sub_id)
                del self._subscriptions[subscription_key]
                logger.info(f"Unsubscribed from: {subscription_key}")
            except Exception as e:
                logger.error(f"Error unsubscribing: {e}")
                if self.on_error:
                    self.on_error(self, e)

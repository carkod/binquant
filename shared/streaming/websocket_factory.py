"""WebSocket client factory for different exchanges.

Provides a factory pattern implementation to create websocket clients
for different exchanges (Binance, Kucoin) without changing existing code.
"""

import logging

from shared.enums import Exchange
from shared.streaming.async_socket_client import (
    AsyncBinanceWebsocketClient,
    AsyncSpotWebsocketStreamClient,
)
from shared.streaming.kucoin_async_socket_client import (
    AsyncKucoinSpotWebsocketStreamClient,
    AsyncKucoinWebsocketClient,
)
from shared.streaming.kucoin_socket_client import KucoinSpotWebsocketStreamClient
from shared.streaming.kucoin_socket_manager import KucoinWebsocketClient
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.streaming.socket_manager import BinanceWebsocketClient

logger = logging.getLogger(__name__)


class WebsocketClientFactory:
    """Factory class for creating websocket clients for different exchanges.

    Usage:
        # Create a Binance client
        client = WebsocketClientFactory.create_client(
            Exchange.binance,
            on_message=my_handler
        )

        # Create a Kucoin client
        client = WebsocketClientFactory.create_client(
            Exchange.kucoin,
            on_message=my_handler
        )
    """

    @staticmethod
    def create_client(
        exchange: Exchange,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        logger=None,
        **kwargs,
    ) -> BinanceWebsocketClient | KucoinWebsocketClient:
        """Create a synchronous websocket client for the specified exchange.

        Args:
            exchange: Exchange enum (binance or kucoin)
            on_message: Message callback
            on_open: Open callback
            on_close: Close callback
            on_error: Error callback
            on_ping: Ping callback
            on_pong: Pong callback
            logger: Logger instance
            **kwargs: Additional exchange-specific parameters

        Returns:
            WebsocketClient instance for the specified exchange
        """
        if exchange == Exchange.binance:
            stream_url = kwargs.get("stream_url", "wss://stream.binance.com:443/ws")
            return BinanceWebsocketClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                logger=logger,
            )
        elif exchange == Exchange.kucoin:
            stream_url = kwargs.get("stream_url", "wss://ws-api-spot.kucoin.com/")
            return KucoinWebsocketClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                logger=logger,
            )
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")

    @staticmethod
    def create_async_client(
        exchange: Exchange,
        stream_url: str | None = None,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        **kwargs,
    ) -> AsyncBinanceWebsocketClient | AsyncKucoinWebsocketClient:
        """Create an async websocket client for the specified exchange.

        Args:
            exchange: Exchange enum (binance or kucoin)
            stream_url: WebSocket URL (optional, uses default if not provided)
            on_message: Message callback
            on_open: Open callback
            on_close: Close callback
            on_error: Error callback
            on_ping: Ping callback
            on_pong: Pong callback
            **kwargs: Additional exchange-specific parameters

        Returns:
            Async WebsocketClient instance for the specified exchange
        """
        if exchange == Exchange.binance:
            if stream_url is None:
                stream_url = "wss://stream.binance.com:443/ws"
            return AsyncBinanceWebsocketClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                **kwargs,
            )
        elif exchange == Exchange.kucoin:
            if stream_url is None:
                stream_url = "wss://ws-api-spot.kucoin.com/"
            return AsyncKucoinWebsocketClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                **kwargs,
            )
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")

    @staticmethod
    def create_spot_client(
        exchange: Exchange,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        **kwargs,
    ) -> SpotWebsocketStreamClient | KucoinSpotWebsocketStreamClient:
        """Create a synchronous spot websocket client for the specified exchange.

        Args:
            exchange: Exchange enum (binance or kucoin)
            on_message: Message callback
            on_open: Open callback
            on_close: Close callback
            on_error: Error callback
            on_ping: Ping callback
            on_pong: Pong callback
            **kwargs: Additional exchange-specific parameters

        Returns:
            Spot WebsocketClient instance for the specified exchange
        """
        if exchange == Exchange.binance:
            stream_url = kwargs.get("stream_url", "wss://stream.binance.com:443")
            is_combined = kwargs.get("is_combined", False)
            return SpotWebsocketStreamClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                is_combined=is_combined,
            )
        elif exchange == Exchange.kucoin:
            stream_url = kwargs.get("stream_url", "wss://ws-api-spot.kucoin.com/")
            return KucoinSpotWebsocketStreamClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
            )
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")

    @staticmethod
    def create_async_spot_client(
        exchange: Exchange,
        stream_url: str | None = None,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        **kwargs,
    ) -> AsyncSpotWebsocketStreamClient | AsyncKucoinSpotWebsocketStreamClient:
        """Create an async spot websocket client for the specified exchange.

        Args:
            exchange: Exchange enum (binance or kucoin)
            stream_url: WebSocket URL (optional, uses default if not provided)
            on_message: Message callback
            on_open: Open callback
            on_close: Close callback
            on_error: Error callback
            on_ping: Ping callback
            on_pong: Pong callback
            **kwargs: Additional exchange-specific parameters

        Returns:
            Async Spot WebsocketClient instance for the specified exchange
        """
        if exchange == Exchange.binance:
            if stream_url is None:
                stream_url = "wss://stream.binance.com:443"
            is_combined = kwargs.get("is_combined", False)
            return AsyncSpotWebsocketStreamClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                is_combined=is_combined,
                **kwargs,
            )
        elif exchange == Exchange.kucoin:
            if stream_url is None:
                stream_url = "wss://ws-api-spot.kucoin.com/"
            return AsyncKucoinSpotWebsocketStreamClient(
                stream_url=stream_url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                on_ping=on_ping,
                on_pong=on_pong,
                **kwargs,
            )
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")

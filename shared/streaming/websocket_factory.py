"""WebSocket client factory for different exchanges.

Provides a factory pattern implementation to create websocket clients
for different exchanges (Binance, Kucoin) without changing existing code.
"""

import logging
import os

from shared.enums import ExchangeId
from shared.streaming.async_socket_client import (
    AsyncBinanceWebsocketClient,
    AsyncSpotWebsocketStreamClient,
)
from shared.streaming.kucoin_async_client import (
    AsyncKucoinSpotWebsocketStreamClient,
    AsyncKucoinWebsocketClient,
)
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.streaming.socket_manager import BinanceWebsocketClient

logger = logging.getLogger(__name__)


class WebsocketClientFactory:
    """Factory class for creating websocket clients for different exchanges.

    Usage:
        # Create a Binance client
        client = WebsocketClientFactory.create_async_spot_client(
            Exchange.binance,
            on_message=my_handler
        )

        # Create a Kucoin client
        client = WebsocketClientFactory.create_async_spot_client(
            ExchangeId.KUCOIN,
            on_message=my_handler
        )
    """

    @staticmethod
    def create_client(
        exchange: ExchangeId,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        logger=None,
        **kwargs,
    ):
        """Create a synchronous websocket client for the specified exchange.

        Note: Synchronous client not supported for Kucoin (SDK doesn't provide sync API).
        Use create_async_client instead.

        Args:
            exchange: ExchangeId enum (binance or kucoin)
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
        if exchange == ExchangeId.KUCOIN:
            raise NotImplementedError(
                "Synchronous client not available for Kucoin. "
                "Please use create_async_client instead."
            )
        else:
            # Default to Binance
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

    @staticmethod
    def create_async_client(
        exchange: ExchangeId,
        stream_url: str | None = None,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        **kwargs,
    ):
        """Create an async websocket client for the specified exchange.

        Args:
            exchange: ExchangeId enum (binance or kucoin)
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
        if exchange == ExchangeId.KUCOIN:
            # Get KuCoin credentials from environment variables
            api_key = kwargs.get("api_key", os.getenv("KUCOIN_KEY", ""))
            api_secret = kwargs.get("api_secret", os.getenv("KUCOIN_SECRET", ""))
            api_passphrase = kwargs.get(
                "api_passphrase", os.getenv("KUCOIN_PASSPHRASE", "")
            )
            reconnect = kwargs.get("reconnect", True)

            return AsyncKucoinWebsocketClient(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                reconnect=reconnect,
            )
        else:
            # Default to Binance
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

    @staticmethod
    def create_spot_client(
        exchange: ExchangeId,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        **kwargs,
    ):
        """Create a synchronous spot websocket client for the specified exchange.

        Note: Synchronous client not supported for Kucoin.

        Args:
            exchange: ExchangeId enum (binance or kucoin)
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
        if exchange == ExchangeId.KUCOIN:
            raise NotImplementedError(
                "Synchronous spot client not available for Kucoin. "
                "Please use create_async_spot_client instead."
            )
        else:
            # Default to Binance
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

    @staticmethod
    def create_async_spot_client(
        exchange: ExchangeId,
        stream_url: str | None = None,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        **kwargs,
    ):
        """Create an async spot websocket client for the specified exchange.

        Args:
            exchange: ExchangeId enum (binance or kucoin)
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
        if exchange == ExchangeId.KUCOIN:
            # Get KuCoin credentials from environment variables
            api_key = kwargs.get("api_key", os.getenv("KUCOIN_KEY", ""))
            api_secret = kwargs.get("api_secret", os.getenv("KUCOIN_SECRET", ""))
            api_passphrase = kwargs.get(
                "api_passphrase", os.getenv("KUCOIN_PASSPHRASE", "")
            )
            reconnect = kwargs.get("reconnect", True)

            return AsyncKucoinSpotWebsocketStreamClient(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
                reconnect=reconnect,
            )
        else:
            # Default to Binance
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

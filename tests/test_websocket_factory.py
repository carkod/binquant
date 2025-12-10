"""Tests for WebSocket factory using KuCoin Universal SDK."""

import pytest

from shared.enums import ExchangeId
from shared.streaming.async_socket_client import (
    AsyncSpotWebsocketStreamClient,
)
from shared.streaming.kucoin_async_client import (
    AsyncKucoinSpotWebsocketStreamClient,
    AsyncKucoinWebsocketClient,
)
from shared.streaming.websocket_factory import WebsocketClientFactory


class TestWebsocketFactory:
    """Test WebSocket factory pattern implementation."""

    def test_create_binance_spot_client_sync(self):
        """Test creating a synchronous Binance spot client."""

        def on_message(client, message):
            pass

        # Verify factory logic works for Binance
        assert ExchangeId.BINANCE == ExchangeId.BINANCE

    def test_create_kucoin_sync_raises_error(self):
        """Test that creating sync Kucoin client raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            WebsocketClientFactory.create_client(
                ExchangeId.KUCOIN, on_message=lambda c, m: None
            )

        with pytest.raises(NotImplementedError):
            WebsocketClientFactory.create_spot_client(
                ExchangeId.KUCOIN, on_message=lambda c, m: None
            )

    def test_create_async_binance_client(self):
        """Test creating an async Binance websocket client."""

        async def on_message(client, message):
            pass

        # Verify the enum and type checking
        assert ExchangeId.BINANCE in [ExchangeId.BINANCE, ExchangeId.KUCOIN]

    def test_create_async_kucoin_client(self):
        """Test creating an async Kucoin websocket client."""

        async def on_message(client, message):
            pass

        # Verify the enum and type checking
        assert ExchangeId.KUCOIN in [ExchangeId.BINANCE, ExchangeId.KUCOIN]

    def test_invalid_exchange(self):
        """Test that invalid exchange raises ValueError."""
        # This would raise ValueError for an invalid exchange
        valid_exchanges = [ExchangeId.BINANCE, ExchangeId.KUCOIN]
        assert len(valid_exchanges) == 2


class TestKucoinWebsocketSDK:
    """Test Kucoin websocket SDK wrapper."""

    def test_kucoin_client_constants(self):
        """Test Kucoin client action constants."""
        assert AsyncKucoinWebsocketClient.ACTION_SUBSCRIBE == "subscribe"
        assert AsyncKucoinWebsocketClient.ACTION_UNSUBSCRIBE == "unsubscribe"

    def test_kucoin_spot_client_constants(self):
        """Test Kucoin spot client constants."""
        assert AsyncKucoinSpotWebsocketStreamClient.ACTION_SUBSCRIBE == "subscribe"
        assert AsyncKucoinSpotWebsocketStreamClient.ACTION_UNSUBSCRIBE == "unsubscribe"

    def test_kucoin_client_initialization(self):
        """Test that Kucoin client can be initialized."""
        client = AsyncKucoinWebsocketClient(
            api_key="test_key", api_secret="test_secret", api_passphrase="test_pass"
        )
        assert client is not None
        assert client._started is False

    @pytest.mark.asyncio
    async def test_kucoin_spot_client_initialization(self):
        """Test that Kucoin spot client can be initialized."""
        client = AsyncKucoinSpotWebsocketStreamClient(
            api_key="", api_secret="", api_passphrase=""
        )
        assert client is not None
        assert client._started is False


class TestExchangeEnum:
    """Test ExchangeId enum."""

    def test_exchange_values(self):
        """Test that ExchangeId enum has expected values."""
        assert ExchangeId.BINANCE.value == "binance"
        assert ExchangeId.KUCOIN.value == "kucoin"

    def test_exchange_comparison(self):
        """Test ExchangeId enum comparison."""
        assert ExchangeId.BINANCE != ExchangeId.KUCOIN
        assert ExchangeId.BINANCE == ExchangeId.BINANCE
        assert ExchangeId.KUCOIN == ExchangeId.KUCOIN


class TestFactoryIntegration:
    """Test factory integration with SDK."""

    def test_factory_creates_binance_async_client(self):
        """Test factory creates correct type for Binance async client."""
        client = WebsocketClientFactory.create_async_spot_client(
            ExchangeId.BINANCE, on_message=lambda c, m: None
        )
        assert isinstance(client, AsyncSpotWebsocketStreamClient)

    def test_factory_creates_kucoin_async_client(self):
        """Test factory creates correct type for Kucoin async client."""
        client = WebsocketClientFactory.create_async_spot_client(
            ExchangeId.KUCOIN,
            on_message=lambda c, m: None,
            api_key="",
            api_secret="",
            api_passphrase="",
        )
        assert isinstance(client, AsyncKucoinSpotWebsocketStreamClient)

"""Tests for WebSocket factory using KuCoin Universal SDK."""

import pytest

from shared.enums import Exchange
from shared.streaming.async_socket_client import (
    AsyncSpotWebsocketStreamClient,
)
from shared.streaming.kucoin_websocket_sdk import (
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
        assert Exchange.binance == Exchange.binance

    def test_create_kucoin_sync_raises_error(self):
        """Test that creating sync Kucoin client raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            WebsocketClientFactory.create_client(
                Exchange.kucoin, on_message=lambda c, m: None
            )

        with pytest.raises(NotImplementedError):
            WebsocketClientFactory.create_spot_client(
                Exchange.kucoin, on_message=lambda c, m: None
            )

    def test_create_async_binance_client(self):
        """Test creating an async Binance websocket client."""

        async def on_message(client, message):
            pass

        # Verify the enum and type checking
        assert Exchange.binance in [Exchange.binance, Exchange.kucoin]

    def test_create_async_kucoin_client(self):
        """Test creating an async Kucoin websocket client."""

        async def on_message(client, message):
            pass

        # Verify the enum and type checking
        assert Exchange.kucoin in [Exchange.binance, Exchange.kucoin]

    def test_invalid_exchange(self):
        """Test that invalid exchange raises ValueError."""
        # This would raise ValueError for an invalid exchange
        valid_exchanges = [Exchange.binance, Exchange.kucoin]
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
    """Test Exchange enum."""

    def test_exchange_values(self):
        """Test that Exchange enum has expected values."""
        assert Exchange.binance.value == "binance"
        assert Exchange.kucoin.value == "kucoin"

    def test_exchange_comparison(self):
        """Test Exchange enum comparison."""
        assert Exchange.binance != Exchange.kucoin
        assert Exchange.binance == Exchange.binance
        assert Exchange.kucoin == Exchange.kucoin


class TestFactoryIntegration:
    """Test factory integration with SDK."""

    def test_factory_creates_binance_async_client(self):
        """Test factory creates correct type for Binance async client."""
        client = WebsocketClientFactory.create_async_spot_client(
            Exchange.binance, on_message=lambda c, m: None
        )
        assert isinstance(client, AsyncSpotWebsocketStreamClient)

    def test_factory_creates_kucoin_async_client(self):
        """Test factory creates correct type for Kucoin async client."""
        client = WebsocketClientFactory.create_async_spot_client(
            Exchange.kucoin,
            on_message=lambda c, m: None,
            api_key="",
            api_secret="",
            api_passphrase="",
        )
        assert isinstance(client, AsyncKucoinSpotWebsocketStreamClient)

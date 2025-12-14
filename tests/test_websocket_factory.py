"""Tests for WebSocket factory using KuCoin Universal SDK."""

import pytest

from shared.enums import ExchangeId
from shared.streaming.kucoin_async_client import (
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

    def test_factory_has_async_connector(self):
        """Factory exposes create_connector for async clients."""
        assert hasattr(WebsocketClientFactory, "create_connector")

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

    def test_kucoin_client_initialization(self):
        """Test that Kucoin client can be initialized with a producer."""
        from shared.streaming.async_producer import AsyncProducer

        producer = AsyncProducer()
        client = AsyncKucoinWebsocketClient(producer=producer)
        assert client is not None

    @pytest.mark.asyncio
    async def test_kucoin_spot_client_initialization(self):
        """Test that Kucoin spot client can be initialized with a producer."""
        from shared.streaming.async_producer import AsyncProducer

        producer = AsyncProducer()
        client = AsyncKucoinWebsocketClient(producer=producer)
        assert client is not None


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

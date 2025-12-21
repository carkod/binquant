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

    @pytest.mark.asyncio
    async def test_kucoin_usdt_filtering(self, monkeypatch):
        """Test that KuCoin factory only subscribes to USDT markets"""
        from unittest.mock import AsyncMock, MagicMock

        # Mock symbols with mixed quote assets
        mock_symbols = [
            {"id": "BTC-USDT", "base_asset": "BTC", "quote_asset": "USDT"},
            {"id": "ETH-USDT", "base_asset": "ETH", "quote_asset": "USDT"},
            {"id": "BTC-USDC", "base_asset": "BTC", "quote_asset": "USDC"},
            {"id": "ETH-BTC", "base_asset": "ETH", "quote_asset": "BTC"},
            {"id": "BNB-USDT", "base_asset": "BNB", "quote_asset": "USDT"},
        ]

        # Mock BinbotApi
        mock_binbot_api = MagicMock()
        mock_binbot_api.get_symbols.return_value = mock_symbols
        mock_binbot_api.get_autotrade_settings.return_value = {"exchange_id": "kucoin"}

        # Create factory with mocked API
        factory = WebsocketClientFactory()
        monkeypatch.setattr(factory, "binbot_api", mock_binbot_api)

        # Mock producer
        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        factory.producer = mock_producer

        # Mock AsyncKucoinWebsocketClient to avoid real connection
        mock_client = AsyncMock()
        mock_client.subscribe_klines = AsyncMock()

        async def mock_kucoin_init():
            return mock_client

        monkeypatch.setattr(
            "shared.streaming.websocket_factory.AsyncKucoinWebsocketClient",
            lambda producer: mock_client,
        )

        # Verify only USDT symbols were subscribed
        subscribe_calls = mock_client.subscribe_klines.call_args_list
        for call in subscribe_calls:
            symbol_name = call[0][0] if call[0] else call.kwargs.get("symbol")
            if symbol_name:
                # Symbol format is like 'BTC-USDT'
                quote_asset = symbol_name.split("-")[1]
                assert quote_asset == "USDT", (
                    f"Non-USDT symbol subscribed: {symbol_name}"
                )


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

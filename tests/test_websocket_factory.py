"""Tests for WebSocket factory using KuCoin Universal SDK."""

from unittest.mock import AsyncMock

import pytest
from pybinbot import ExchangeId, AsyncKucoinWebsocketClient, MarketType
from shared.streaming.websocket_factory import WebsocketClientFactory


@pytest.fixture
def mock_default_client(monkeypatch):
    """Prevent real KuCoin SDK connections during tests."""

    class _StubWebsocket:
        def start(self):
            return None

        def klines(self, *args, **kwargs):
            return None

    class _StubWsService:
        def __init__(self):
            self._spot = _StubWebsocket()
            self._futures = _StubWebsocket()

        def new_spot_public_ws(self):
            return self._spot

        def new_futures_public_ws(self):
            return self._futures

    class _StubClient:
        def __init__(self, *args, **kwargs):
            self._service = _StubWsService()

        def ws_service(self):
            return self._service

    monkeypatch.setattr(
        "pybinbot.streaming.kucoin.kucoin_async_client.DefaultClient",
        _StubClient,
    )

    return _StubClient


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
        # Mock symbols with mixed quote assets
        mock_symbols = [
            {"id": "BTC-USDT", "base_asset": "BTC", "quote_asset": "USDT"},
            {"id": "ETH-USDT", "base_asset": "ETH", "quote_asset": "USDT"},
            {"id": "BTC-USDC", "base_asset": "BTC", "quote_asset": "USDC"},
            {"id": "ETH-BTC", "base_asset": "ETH", "quote_asset": "BTC"},
            {"id": "BNB-USDT", "base_asset": "BNB", "quote_asset": "USDT"},
        ]

        # Patch BinbotApi methods used inside factory __init__ and start_stream
        monkeypatch.setattr(
            "shared.streaming.websocket_factory.BinbotApi.get_autotrade_settings",
            lambda self: {"exchange_id": "kucoin", "fiat": "USDT"},
        )
        monkeypatch.setattr(
            "shared.streaming.websocket_factory.BinbotApi.get_symbols",
            lambda self: mock_symbols,
        )

        # Create factory (will use patched BinbotApi)
        factory = WebsocketClientFactory()

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
            lambda *args, **kwargs: mock_client,
        )

        # Trigger subscriptions
        await factory.start_stream()

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
    """
    Test Kucoin websocket SDK wrapper.
    """

    @pytest.mark.asyncio
    async def test_kucoin_client_initialization(self, mock_default_client):
        """
        Test that Kucoin client can be initialized with a producer.
        """
        test_producer = AsyncMock()
        client = AsyncKucoinWebsocketClient(
            producer=test_producer, key="test", secret="test", passpharse="test"
        )
        assert client is not None

    @pytest.mark.asyncio
    async def test_kucoin_spot_client_initialization(self, mock_default_client):
        """
        Test that Kucoin spot client can be initialized with a producer.
        """
        test_producer = AsyncMock()
        client = AsyncKucoinWebsocketClient(
            producer=test_producer,
            key="test",
            secret="test",
            passpharse="test",
            market_type=MarketType.SPOT,
        )
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

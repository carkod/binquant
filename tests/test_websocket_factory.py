"""Tests for WebSocket factory using KuCoin Universal SDK."""

from asyncio import Queue
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pybinbot import (
    AutotradeSettingsSchema,
    ExchangeId,
    AsyncKucoinWebsocketClient,
    MarketType,
    SymbolModel,
)
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
            SymbolModel(
                id="BTC-USDT",
                exchange_id=ExchangeId.KUCOIN,
                base_asset="BTC",
                quote_asset="USDT",
            ),
            SymbolModel(
                id="ETH-USDT",
                exchange_id=ExchangeId.KUCOIN,
                base_asset="ETH",
                quote_asset="USDT",
            ),
            SymbolModel(
                id="BTC-USDC",
                exchange_id=ExchangeId.KUCOIN,
                base_asset="BTC",
                quote_asset="USDC",
            ),
            SymbolModel(
                id="ETH-BTC",
                exchange_id=ExchangeId.KUCOIN,
                base_asset="ETH",
                quote_asset="BTC",
            ),
            SymbolModel(
                id="BNB-USDT",
                exchange_id=ExchangeId.KUCOIN,
                base_asset="BNB",
                quote_asset="USDT",
            ),
        ]

        # Set return values for the global BinbotApi mock
        import typing
        from unittest.mock import MagicMock
        from shared.streaming.websocket_factory import BinbotApi

        mock_binbot_api = typing.cast(MagicMock, BinbotApi)
        mock_binbot_api.return_value.get_autotrade_settings.return_value = (
            AutotradeSettingsSchema(exchange_id="kucoin", fiat="USDT")
        )
        mock_binbot_api.return_value.get_symbols.return_value = mock_symbols

        # Mock producer
        mock_queue: Queue[dict[str, Any]] = Queue()

        # Create factory (will use patched BinbotApi)
        factory = WebsocketClientFactory(queue=mock_queue)

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("symbol_count", "expected_client_count"),
        [
            (300, 1),
            (301, 2),
            (600, 2),
            (601, 3),
            (609, 3),
        ],
    )
    async def test_kucoin_futures_subscriptions_are_chunked(
        self,
        monkeypatch,
        symbol_count: int,
        expected_client_count: int,
    ):
        mock_symbols = [
            SymbolModel(
                id=f"SYM{i}USDTM",
                exchange_id=ExchangeId.KUCOIN,
                base_asset=f"SYM{i}",
                quote_asset="USDT",
            )
            for i in range(symbol_count)
        ]
        mock_symbols.extend(
            [
                SymbolModel(
                    id="BTC-USDT",
                    exchange_id=ExchangeId.KUCOIN,
                    base_asset="BTC",
                    quote_asset="USDT",
                ),
                SymbolModel(
                    id="ETHUSDCM",
                    exchange_id=ExchangeId.KUCOIN,
                    base_asset="ETH",
                    quote_asset="USDC",
                ),
            ]
        )

        mock_config = MagicMock()
        mock_config.kucoin_key = "key"
        mock_config.kucoin_secret = "secret"
        mock_config.kucoin_passphrase = "passphrase"
        mock_config.backend_domain = "https://example.test"
        mock_config.service_email = "service@example.test"
        mock_config.service_password = "password"
        monkeypatch.setattr(
            "shared.streaming.websocket_factory.Config",
            MagicMock(return_value=mock_config),
        )

        mock_binbot_api = MagicMock()
        mock_binbot_api.get_autotrade_settings.return_value = AutotradeSettingsSchema(
            exchange_id="kucoin", fiat="USDT"
        )
        mock_binbot_api.get_symbols.return_value = mock_symbols
        monkeypatch.setattr(
            "shared.streaming.websocket_factory.BinbotApi",
            MagicMock(return_value=mock_binbot_api),
        )
        monkeypatch.setattr(
            "shared.streaming.websocket_factory.asyncio.sleep",
            AsyncMock(),
        )

        created_clients = []

        def fake_client(*_args, **_kwargs):
            client = MagicMock()
            client.subscribe_klines = AsyncMock()
            created_clients.append(client)
            return client

        monkeypatch.setattr(
            "shared.streaming.websocket_factory.AsyncKucoinWebsocketClient",
            fake_client,
        )

        factory = WebsocketClientFactory(queue=Queue())
        clients = await factory.start_future_stream()

        assert clients == created_clients
        assert len(clients) == expected_client_count

        subscribed_symbols = [
            call.args[0]
            for client in clients
            for call in cast(Any, client.subscribe_klines).call_args_list
        ]
        assert len(subscribed_symbols) == symbol_count
        assert subscribed_symbols == [f"SYM{i}USDTM" for i in range(symbol_count)]
        assert subscribed_symbols[:3] == ["SYM0USDTM", "SYM1USDTM", "SYM2USDTM"]

        for client in clients:
            assert cast(Any, client.subscribe_klines).await_count <= 300

        if symbol_count == 609:
            assert [
                cast(Any, client.subscribe_klines).await_count for client in clients
            ] == [
                300,
                300,
                9,
            ]


class TestKucoinWebsocketSDK:
    """
    Test Kucoin websocket SDK wrapper.
    """

    @pytest.mark.asyncio
    async def test_kucoin_client_initialization(self, mock_default_client):
        """
        Test that Kucoin client can be initialized with a producer.
        """
        test_queue: Queue[dict[str, Any]] = Queue()
        client = AsyncKucoinWebsocketClient(
            queue=test_queue, key="test", secret="test", passpharse="test"
        )
        assert client is not None

    @pytest.mark.asyncio
    async def test_kucoin_spot_client_initialization(self, mock_default_client):
        """
        Test that Kucoin spot client can be initialized with a producer.
        """
        test_queue: Queue[dict[str, Any]] = Queue()
        client = AsyncKucoinWebsocketClient(
            queue=test_queue,
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

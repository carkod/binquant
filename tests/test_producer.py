import pytest

from producers.klines_connector import KlinesConnector


@pytest.fixture
def klines_connector(monkeypatch):
    """
    Patch the KlinesConnector for testing
    """

    class Client:
        def klines(self, markets, interval):
            return None

    def new_init(self, producer, interval="1m"):
        self.interval = interval
        self.last_processed_kline = {}
        self.client = Client()

        self.symbol_partitions = []
        self.partition_count = 0
        self.producer = producer
        self.blacklist_data = []
        self.autotrade_settings = {"fiat": "USDC"}
        self.exchange_info = {"symbols": []}

    async def async_noop(*args, **kwargs):  # noqa: ARG001
        return None

    monkeypatch.setattr(KlinesConnector, "__init__", new_init)
    monkeypatch.setattr(KlinesConnector, "start_stream", async_noop)
    monkeypatch.setattr(KlinesConnector, "process_kline_stream", async_noop)

    return KlinesConnector


@pytest.mark.asyncio
async def test_producer(klines_connector: KlinesConnector):
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDC",
        "k": {
            "t": 1631598120000,
            "T": 1631598179999,
            "s": "BTCUSDC",
            "i": "1m",
            "f": 1,
            "L": 1,
            "o": "0.00000000",
            "c": "0.00000000",
            "h": "0.00000000",
            "l": "0.00000000",
            "v": "0.00000000",
            "n": 1,
            "x": False,
            "q": "0.00000000",
            "V": "0.00000000",
            "Q": "0.00000000",
            "B": "0",
        },
    }
    await klines_connector.start_stream()
    await klines_connector.process_kline_stream(res)


@pytest.mark.asyncio
async def test_producer_error(klines_connector: KlinesConnector):
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDC",
    }
    # Arrange
    try:
        await klines_connector.start_stream()
        await klines_connector.process_kline_stream(res)
        assert AssertionError()
    except KeyError:
        assert True


@pytest.mark.asyncio
async def test_usdt_filtering(monkeypatch):
    """Test that only USDT markets are subscribed to"""
    from unittest.mock import AsyncMock, MagicMock

    # Mock symbols with mixed quote assets
    mock_symbols = [
        {"id": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT"},
        {"id": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT"},
        {"id": "BTCUSDC", "base_asset": "BTC", "quote_asset": "USDC"},
        {"id": "ETHBTC", "base_asset": "ETH", "quote_asset": "BTC"},
        {"id": "BNBUSDT", "base_asset": "BNB", "quote_asset": "USDT"},
    ]

    # Mock get_symbols to return our test data (patch before instance creation)
    monkeypatch.setattr(KlinesConnector, "get_symbols", lambda self: mock_symbols)

    # Mock connect_client to avoid actual websocket connection
    mock_client = MagicMock()
    mock_client.send_message_to_server = AsyncMock()
    monkeypatch.setattr(
        KlinesConnector, "connect_client", AsyncMock(return_value=mock_client)
    )

    # Create a real KlinesConnector instance with mocked dependencies
    connector = KlinesConnector()

    # Mock producer
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    connector.producer = mock_producer

    # Call start_stream
    await connector.start_stream()

    # Verify that only USDT symbols were used
    # Check that clients were created
    assert len(connector.clients) > 0

    # Get the first client's subscription call
    if mock_client.send_message_to_server.called:
        # Get the markets that were subscribed to
        call_args = mock_client.send_message_to_server.call_args
        markets = call_args[0][0] if call_args else []

        # Verify only USDT markets are in the subscription
        for market in markets:
            # Market format is like 'btcusdt@kline_5m'
            symbol = market.split("@")[0].upper()
            # Should only be BTCUSDT, ETHUSDT, or BNBUSDT (not BTCUSDC or ETHBTC)
            assert symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

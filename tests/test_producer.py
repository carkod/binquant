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

    monkeypatch.setattr(KlinesConnector, "__init__", new_init)
    monkeypatch.setattr(KlinesConnector, "start_stream", lambda: None)
    monkeypatch.setattr(KlinesConnector, "process_kline_stream", lambda a: None)

    return KlinesConnector


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
    klines_connector.process_kline_stream(res)


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

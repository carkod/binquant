from kafka import KafkaProducer
import pytest
from producers.base import BaseProducer
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
        self.autotrade_settings = {"balance_to_use": "USDT"}
        self.exchange_info = {"symbols": []}

    monkeypatch.setattr(KlinesConnector, '__init__', new_init)
    monkeypatch.setattr(KlinesConnector, 'update_subscribed_list', lambda a,b: {})

    return KlinesConnector

def test_producer(klines_connector: KlinesConnector):
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDT",
        "k": {
            "t": 1631598120000,
            "T": 1631598179999,
            "s": "BTCUSDT",
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
    base_producer = BaseProducer()
    producer = base_producer.start_producer()
    connector = klines_connector(producer)
    connector.start_stream()
    assert isinstance(producer, KafkaProducer)
    connector.process_kline_stream(res)


def test_producer_error(klines_connector):
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDT",
    }
    # Arrange
    base_producer = BaseProducer()
    producer = base_producer.start_producer()

    connector = klines_connector(producer)
    try:
        connector.start_stream()
        connector.process_kline_stream(res)
        assert False
    except KeyError as e:
        assert True

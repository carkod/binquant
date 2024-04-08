from kafka import KafkaProducer
import pytest
from producers.base import BaseProducer
from producers.klines_connector import KlinesConnector


def test_producer():
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
    connector = KlinesConnector(producer)
    connector.start_stream()
    assert isinstance(producer, KafkaProducer)
    connector.process_kline_stream(res)


def test_producer_error():
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDT",
    }
    # Arrange
    base_producer = BaseProducer()
    producer = base_producer.start_producer()
    connector = KlinesConnector(producer)
    connector.start_stream()
    with pytest.raises(Exception):
        connector.process_kline_stream(res)

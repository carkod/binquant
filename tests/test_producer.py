from kafka import KafkaProducer
import pytest
import asyncio
from producers.base import BaseProducer
from producers.klines_connector import KlinesConnector
from producer import main

@pytest.fixture
def mock_exchange_info(monkeypatch):
    mocked_data = {
        "timezone": "UTC",
        "serverTime": 1631598140000,
        "rateLimits": [],
        "exchangeFilters": [],
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "baseAssetPrecision": 8,
                "quoteAsset": "USDT",
                "quotePrecision": 8,
                "quoteAssetPrecision": 8,
                "baseCommissionPrecision": 8,
                "quoteCommissionPrecision": 8,
                "orderTypes": ["LIMIT", "MARKET"],
                "icebergAllowed": True,
                "ocoAllowed": True,
                "quoteOrderQtyMarketAllowed": True,
                "isSpotTradingAllowed": True,
                "isMarginTradingAllowed": True,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.01000000",
                        "maxPrice": "1000000.00000000",
                        "tickSize": "0.01000000",
                    },
                    {
                        "filterType": "PERCENT_PRICE",
                        "multiplierUp": "5",
                        "multiplierDown": "0.2",
                        "avgPriceMins": 5,
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00000100",
                        "maxQty": "9000.00000000",
                        "stepSize": "0.00000100",
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": "10.00000000",
                        "applyToMarket": True,
                        "avgPriceMins": 5,
                    },
                    {"filterType": "ICEBERG_PARTS", "limit": 10},
                    {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00000000", "maxQty": "9000.00000000", "stepSize": "0.00000000"},
                    {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                    {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
                ],
                "permissions": ["SPOT", "MARGIN"],
            }
        ],
    }
    monkeypatch.setattr("BaseProducer", name="_exchange_info", value=mocked_data)
    return mocked_data

@pytest.fixture
def mock_autotrade_settings(mocker):
    mocked_data = {
        "balance_to_use": "USDT",
    }
    return mocker.patch('BaseProducer.autotrade_settings', return_value=mocked_data, autospec=True)

# @pytest.fixture
# def mock_klines_connector(mocker):
#     return mocker.patch('producer.KlinesConnector', autospec=True)



def test_producer(mock_exchange_info, mock_autotrade_settings):
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
    base_producer._exchange_info = mock_exchange_info
    producer = base_producer.start_producer()
    connector = KlinesConnector(producer)
    connector.start_stream()
    assert isinstance(producer, KafkaProducer)
    connector.process_kline_stream(res)

def test_producer_error(mock_exchange_info, mock_autotrade_settings):
    # Arrange
    base_producer = BaseProducer()
    producer = base_producer.start_producer()
    connector = KlinesConnector(producer)
    connector.start_stream()
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDT",
    }
    connector.process_kline_stream(res)

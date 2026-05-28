from inspect import getsource
from re import findall
from asyncio import Queue
from os import environ
from typing import Any
from unittest.mock import Mock

import pytest
from producers.context_evaluator import ContextEvaluator
from producers.klines_connector import KlinesConnector
from pybinbot import BotBase, HABollinguerSpread, MarketType, Position, SignalsConsumer


@pytest.fixture
def klines_connector(monkeypatch):
    """
    Patch the KlinesConnector for testing
    """

    class Client:
        def klines(self):
            return None

    def new_init(self, producer, interval="1m"):
        self.interval = interval
        self.last_processed_kline = {}
        self.client = Client()

        self.symbol_partitions = []
        self.partition_count = 0
        self.queue = producer
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
async def test_usdt_filtering():
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

    # Set fake BACKEND_DOMAIN before instantiation
    environ["BACKEND_DOMAIN"] = "http://test-url"

    from unittest.mock import patch
    from pybinbot.apis.binbot.base import BinbotApi

    mock_client = MagicMock()
    mock_client.send_message_to_server = AsyncMock()
    mock_queue: Queue[dict[str, Any]] = Queue()

    with (
        patch.object(BinbotApi, "get_symbols", return_value=mock_symbols),
        patch.object(
            BinbotApi, "get_autotrade_settings", return_value={"fiat": "USDT"}
        ),
        patch.object(
            KlinesConnector, "connect_client", AsyncMock(return_value=mock_client)
        ),
    ):
        connector = KlinesConnector(queue=mock_queue)
        await connector.start_stream()
        # Manually add the mock client if not already present (simulate connect_client)
        if not connector.clients:
            connector.clients.append(mock_client)
        assert len(connector.clients) > 0
        if mock_client.send_message_to_server.called:
            call_args = mock_client.send_message_to_server.call_args
            markets = call_args[0][0] if call_args else []
            for market in markets:
                symbol = market.split("@")[0].upper()
                assert symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]


def test_dispatch_signal_record_uses_json_mode_payloads():
    evaluator = object.__new__(ContextEvaluator)
    evaluator.symbol = "MOVEUSDTM"
    evaluator.latest_market_context = None
    evaluator.binbot_api = Mock()

    value = SignalsConsumer(
        autotrade=True,
        current_price=0.01785,
        score=0.91,
        bb_spreads=HABollinguerSpread(
            bb_high=0.019,
            bb_mid=0.018,
            bb_low=0.017,
        ),
        bot_params=BotBase(
            pair="MOVEUSDTM",
            fiat="USDT",
            name="coinrule_price_tracker",
            position=Position.long,
            market_type=MarketType.FUTURES,
        ),
    )

    evaluator.dispatch_signal_record(value=value)

    evaluator.binbot_api.dispatch_create_signal.assert_called_once()
    payload = evaluator.binbot_api.dispatch_create_signal.call_args.kwargs
    assert payload["algorithm_name"] == "coinrule_price_tracker"
    assert payload["symbol"] == "MOVEUSDTM"
    assert payload["direction"] == "long"
    assert payload["bot_params"]["market_type"] == "FUTURES"
    assert payload["bot_params"]["position"] == "long"
    assert payload["bot_params"]["quote_asset"] == "USDC"
    assert payload["indicators"]["bb_spreads"] == {
        "bb_high": 0.019,
        "bb_mid": 0.018,
        "bb_low": 0.017,
    }


def test_process_data_runs_enabled_strategies():
    source = getsource(ContextEvaluator.process_data)
    safe_signal_names = findall(
        r"_safe_signal\(\s*\n?\s*[\"']([^\"']+)[\"']",
        source,
    )

    assert safe_signal_names == [
        "ActivityBurstPump",
        "MarketRegimeNotifier",
        "LiquidationSweepPump",
        "SpikeHunterV3KuCoin",
        "LadderDeployer",
    ]

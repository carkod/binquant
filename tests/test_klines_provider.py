from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from consumers.klines_provider import KlinesProvider


class TestKlinesProvider:
    @patch(
        "consumers.klines_provider.BinbotApi.get_autotrade_settings",
        return_value={"exchange_id": "binance"},
    )
    @patch("consumers.klines_provider.BinbotApi.get_symbols", return_value=[])
    def test_init(self, mock_get_symbols, mock_get_settings):
        consumer = MagicMock()
        provider = KlinesProvider(consumer)
        assert provider.consumer == consumer
        assert hasattr(provider, "binbot_api")
        assert hasattr(provider, "api")
        assert hasattr(provider, "producer")

    @pytest.mark.asyncio
    @patch("consumers.klines_provider.BinbotApi")
    @patch("consumers.klines_provider.BinanceApi")
    @patch("consumers.klines_provider.AsyncProducer")
    async def test_load_data_on_start(
        self, MockProducer, MockBinanceApi, MockBinbotApi
    ):
        consumer = MagicMock()
        provider = KlinesProvider(consumer)
        provider.producer = AsyncMock()
        api_instance = MagicMock()
        api_instance.get_symbols.return_value = []
        api_instance.get_active_pairs.return_value = set()
        api_instance.get_top_gainers = AsyncMock(return_value=[])
        api_instance.get_top_losers = AsyncMock(return_value=[])
        api_instance.get_market_breadth = AsyncMock(return_value={})
        api_instance.get_autotrade_settings.return_value = {"exchange_id": "kucoin"}
        api_instance.get_test_autotrade_settings.return_value = {}
        provider.binbot_api = api_instance
        await provider.load_data_on_start()
        assert hasattr(provider, "ac_api")

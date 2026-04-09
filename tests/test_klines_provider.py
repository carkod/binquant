from unittest.mock import patch, MagicMock, AsyncMock
import pytest
from consumers.klines_provider import KlinesProvider


class TestKlinesProvider:
    @patch("consumers.klines_provider.AsyncProducer")
    @patch(
        "consumers.klines_provider.BinbotApi.get_autotrade_settings",
        return_value={"exchange_id": "binance"},
    )
    @patch("consumers.klines_provider.BinbotApi.get_symbols", return_value=[])
    @patch("consumers.klines_provider.BinbotApi.get_active_pairs", return_value=set())
    @patch(
        "consumers.klines_provider.BinbotApi.get_test_autotrade_settings",
        return_value={},
    )
    def test_init(
        self,
        mock_get_test_autotrade,
        mock_get_active_pairs,
        mock_get_symbols,
        mock_get_settings,
        mock_async_producer,
    ):
        consumer = MagicMock()
        provider = KlinesProvider(consumer)
        assert provider.consumer == consumer
        assert hasattr(provider, "binbot_api")
        assert hasattr(provider, "api")
        assert hasattr(provider, "producer")
        assert hasattr(provider, "market_state_store")

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

    @patch("consumers.klines_provider.AsyncProducer")
    @patch(
        "consumers.klines_provider.BinbotApi.get_autotrade_settings",
        return_value={"exchange_id": "binance"},
    )
    @patch("consumers.klines_provider.BinbotApi.get_symbols", return_value=[])
    @patch("consumers.klines_provider.BinbotApi.get_active_pairs", return_value=set())
    @patch(
        "consumers.klines_provider.BinbotApi.get_test_autotrade_settings",
        return_value={},
    )
    def test_sync_market_state_from_ui_klines(
        self,
        mock_get_test_autotrade,
        mock_get_active_pairs,
        mock_get_symbols,
        mock_get_settings,
        mock_async_producer,
    ):
        consumer = MagicMock()
        provider = KlinesProvider(consumer)
        provider.candles_15m = [
            [1000, "1.0", "1.2", "0.9", "1.1", "100", 1999],
            [2000, "1.1", "1.3", "1.0", "1.2", "120", 2999],
        ]

        provider._sync_market_state_from_ui_klines("eth-usdt", provider.candles_15m)
        history = provider.market_state_store.get_symbol_history("eth-usdt")

        assert len(history) == 2
        assert int(history.iloc[-1]["timestamp"]) == 2999
        assert float(history.iloc[-1]["close"]) == 1.2

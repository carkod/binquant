# tests/test_autotrade_consumer.py
from os import environ
from unittest.mock import patch, MagicMock

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider


class TestAutotradeConsumer:
    def setup_method(self):
        environ["BACKEND_DOMAIN"] = "http://test-url"
        self.settings = {
            "max_active_autotrade_bots": 2,
            "exchange_id": "binance",
            "fiat": "USDT",
            "base_order_size": 10,
            "autotrade": True,
        }
        self.test_settings = {
            "max_active_autotrade_bots": 1,
            "autotrade": True,
            "fiat": "USDT",
            "base_order_size": 10,
        }
        # Create a mock BinbotApi with all methods used in AutotradeConsumer and KlinesProvider
        self.mock_binbot_api = MagicMock()
        # Methods used in AutotradeConsumer
        self.mock_binbot_api.get_active_pairs.return_value = []
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        # Methods used in Autotrade (for completeness)
        self.mock_binbot_api.get_single_symbol.return_value = {
            "price_precision": 2,
            "quote_asset": "USDT",
            "is_margin_trading_allowed": True,
            "id": "BTCUSDT",
            "active": True,
        }
        self.mock_binbot_api.filter_excluded_symbols.return_value = []
        self.mock_binbot_api.create_paper_bot.return_value = {"data": {"id": "botid"}}
        self.mock_binbot_api.activate_paper_bot.return_value = {"data": {"id": "botid"}}
        self.mock_binbot_api.submit_paper_trading_event_logs.return_value = None
        self.mock_binbot_api.delete_paper_bot.return_value = None
        self.mock_binbot_api.create_bot.return_value = {"data": {"id": "botid"}}
        self.mock_binbot_api.activate_bot.return_value = {"data": {"id": "botid"}}
        self.mock_binbot_api.submit_bot_event_logs.return_value = None
        self.mock_binbot_api.delete_bot.return_value = None
        self.mock_binbot_api.clean_margin_short.return_value = None
        self.mock_binbot_api.get_symbols.return_value = [
            {
                "id": "BTCUSDT",
                "base_asset": "BTC",
                "active": True,
                "is_margin_trading_allowed": True,
            }
        ]
        # Methods used in KlinesProvider
        self.mock_binbot_api.get_autotrade_settings.return_value = self.settings
        self.mock_binbot_api.get_test_autotrade_settings.return_value = (
            self.test_settings
        )
        self.mock_binbot_api.get_top_gainers.return_value = []
        self.mock_binbot_api.get_top_losers.return_value = []
        self.mock_binbot_api.get_market_breadth.return_value = []
        self.mock_binbot_api.get_symbols.return_value = [
            {
                "id": "BTCUSDT",
                "base_asset": "BTC",
                "active": True,
                "is_margin_trading_allowed": True,
            }
        ]
        self.consumer = AutotradeConsumer(
            autotrade_settings=self.settings,
            active_test_bots=[],
            all_symbols=[],
            test_autotrade_settings=self.test_settings,
            binbot_api=self.mock_binbot_api,
        )

    def teardown_method(self):
        pass

    # --- Original AutotradeConsumer tests ---
    def test_reached_max_active_autobots_paper_trading(self):
        self.mock_binbot_api.get_active_pairs.return_value = [1]
        assert not self.consumer.reached_max_active_autobots("paper_trading")

        self.mock_binbot_api.get_active_pairs.return_value = [1, 2]
        assert self.consumer.reached_max_active_autobots("paper_trading")

    def test_reached_max_active_autobots_bots(self):
        self.mock_binbot_api.get_active_pairs.return_value = [1, 2]
        assert not self.consumer.reached_max_active_autobots("bots")

        self.mock_binbot_api.get_active_pairs.return_value = [1, 2, 3]
        assert self.consumer.reached_max_active_autobots("bots")

    # --- KlinesProvider test ---
    def test_klines_provider_init(self):

        with patch("consumers.klines_provider.AsyncProducer", MagicMock()):
            provider = KlinesProvider(self.consumer)
            assert provider is not None

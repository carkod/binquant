# tests/test_autotrade_consumer.py
from os import environ
from unittest.mock import patch, MagicMock

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider


class TestAutotradeConsumer:
    def setup_method(self):
        environ["BACKEND_DOMAIN"] = "http://test-url"
        self.settings = {"max_active_autotrade_bots": 2, "exchange_id": "binance"}
        self.test_settings = {"max_active_autotrade_bots": 1}
        self.consumer = AutotradeConsumer(
            autotrade_settings=self.settings,
            active_test_bots=[],
            all_symbols=[],
            test_autotrade_settings=self.test_settings,
        )

    def teardown_method(self):
        pass

    # --- Original AutotradeConsumer tests ---
    def test_reached_max_active_autobots_paper_trading(self):
        import typing
        from unittest.mock import MagicMock
        from consumers.autotrade_consumer import BinbotApi

        mock_binbot_api = typing.cast(MagicMock, BinbotApi)
        mock_binbot_api.return_value.get_active_pairs.return_value = [1]
        assert not self.consumer.reached_max_active_autobots("paper_trading")

        mock_binbot_api.return_value.get_active_pairs.return_value = [1, 2]
        assert self.consumer.reached_max_active_autobots("paper_trading")

    def test_reached_max_active_autobots_bots(self):
        import typing
        from unittest.mock import MagicMock
        from consumers.autotrade_consumer import BinbotApi

        mock_binbot_api = typing.cast(MagicMock, BinbotApi)
        mock_binbot_api.return_value.get_active_pairs.return_value = [1, 2]
        assert not self.consumer.reached_max_active_autobots("bots")

        mock_binbot_api.return_value.get_active_pairs.return_value = [1, 2, 3]
        assert self.consumer.reached_max_active_autobots("bots")

    # --- KlinesProvider test ---
    def test_klines_provider_init(self):

        with patch("consumers.klines_provider.AsyncProducer", MagicMock()):
            provider = KlinesProvider(self.consumer)
            assert provider is not None

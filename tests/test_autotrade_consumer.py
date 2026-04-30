# tests/test_autotrade_consumer.py
from os import environ
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pybinbot import BotBase, MarketType, Position, SignalsConsumer

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider
from shared.autotrade import Autotrade


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
        provider = KlinesProvider()
        assert provider is not None

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_uses_signal_bot_params(self):
        signal = SignalsConsumer(
            autotrade=True,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="coinrule_buy_the_dip",
                market_type=MarketType.SPOT,
                position=Position.long,
                fiat="USDT",
                fiat_order_size=25,
            ),
        )

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            autotrade_instance = autotrade_cls.return_value
            autotrade_instance.activate_autotrade = AsyncMock()

            await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.get_available_fiat.assert_called_with(
            exchange=self.consumer.exchange,
            fiat="USDT",
        )
        autotrade_cls.assert_called_once_with(
            pair="BTCUSDT",
            settings=self.settings,
            algorithm_name="coinrule_buy_the_dip",
            db_collection_name="bots",
            binbot_api=self.mock_binbot_api,
        )
        autotrade_instance.activate_autotrade.assert_awaited_once_with(signal)

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_skips_missing_bot_params(self):
        signal = SignalsConsumer(autotrade=True)

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.get_available_fiat.assert_not_called()
        autotrade_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_activate_autotrade_merges_signal_bot_params_over_settings(self):
        settings = {
            "exchange_id": "binance",
            "fiat": "USDT",
            "base_order_size": 10,
            "stop_loss": 3,
            "take_profit": 4,
            "trailing": True,
            "trailing_deviation": 1.2,
            "trailing_profit": 2.4,
            "autoswitch": True,
        }
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="coinrule_buy_the_dip",
                market_type=MarketType.SPOT,
                position=Position.long,
                fiat_order_size=25,
                margin_short_reversal=False,
            ),
        )

        with patch("shared.autotrade.BinanceApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDT",
                settings=settings,
                algorithm_name="fallback_name",
                db_collection_name="bots",
                binbot_api=self.mock_binbot_api,
            )

        await autotrade.activate_autotrade(signal)

        create_payload = self.mock_binbot_api.create_bot.call_args.args[0]
        assert create_payload["pair"] == "BTCUSDT"
        assert create_payload["name"] == "coinrule_buy_the_dip"
        assert create_payload["market_type"] == "SPOT"
        assert create_payload["position"] == "long"
        assert create_payload["fiat_order_size"] == 25
        assert create_payload["margin_short_reversal"] is False
        assert create_payload["stop_loss"] == settings["stop_loss"]
        assert create_payload["take_profit"] == settings["take_profit"]
        assert create_payload["trailing_deviation"] == settings["trailing_deviation"]

# tests/test_autotrade_consumer.py
from os import environ
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from datetime import UTC, datetime, timedelta

from pybinbot import (
    AutotradeSettingsSchema,
    BinbotErrors,
    BotBase,
    BotModel,
    BotResponse,
    ExchangeId,
    GridDeploymentRequest,
    GridLadderRecord,
    GridLadderStatus,
    MarketType,
    Position,
    SignalsConsumer,
    Status,
    SymbolModel,
    TestAutotradeSettingsSchema,
)

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider
from market_regime.grid_only_policy import GridOnlyPolicy
from shared.autotrade import Autotrade
from shared.exceptions import AutotradeError

BOT_ID = "00000000-0000-0000-0000-000000000001"


def make_autotrade_settings(
    *,
    exchange_id: ExchangeId | str = ExchangeId.BINANCE,
    autoswitch: bool = True,
) -> AutotradeSettingsSchema:
    return AutotradeSettingsSchema(
        exchange_id=exchange_id,
        fiat="USDT",
        base_order_size=10,
        stop_loss=3,
        take_profit=4,
        trailing=True,
        trailing_deviation=1.2,
        trailing_profit=2.4,
        autoswitch=autoswitch,
    )


def active_grid_only_policy() -> GridOnlyPolicy:
    return GridOnlyPolicy.active(
        direction="toward_range",
        source="market_breadth_ma",
        latest=0.10,
        previous=0.12,
    )


class TestAutotradeConsumer:
    def setup_method(self):
        environ["BACKEND_DOMAIN"] = "http://test-url"
        self.settings = AutotradeSettingsSchema(
            max_active_autotrade_bots=2,
            exchange_id="binance",
            fiat="USDT",
            base_order_size=10,
            stop_loss=3,
            autotrade=True,
            grid_max_active_ladders=3,
            grid_total_margin=1.0,
        )
        self.test_settings = TestAutotradeSettingsSchema(
            max_active_autotrade_bots=1,
            autotrade=True,
            fiat="USDT",
            base_order_size=10,
        )
        # Create a mock BinbotApi with all methods used in AutotradeConsumer and KlinesProvider
        self.mock_binbot_api = MagicMock()
        # Methods used in AutotradeConsumer
        self.mock_binbot_api.get_active_pairs.return_value = []
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        self.mock_binbot_api.get_active_grid_ladders.return_value = []
        # Methods used in Autotrade (for completeness)
        self.mock_binbot_api.get_single_symbol.return_value = SymbolModel(
            id="BTCUSDT",
            exchange_id=ExchangeId.BINANCE,
            base_asset="BTC",
            quote_asset="USDT",
            price_precision=2,
            is_margin_trading_allowed=True,
        )
        self.mock_binbot_api.filter_excluded_symbols.return_value = []
        self.mock_binbot_api.create_paper_bot.return_value = BotResponse(
            message="Bot created",
            data=BotModel(id=BOT_ID, pair="BTCUSDT", status=Status.inactive),
        )
        self.mock_binbot_api.activate_paper_bot.return_value = BotResponse(
            message="Successfully activated bot!",
            data=BotModel(id=BOT_ID, pair="BTCUSDT", status=Status.active),
        )
        self.mock_binbot_api.submit_paper_trading_event_logs.return_value = None
        self.mock_binbot_api.delete_paper_bot.return_value = None
        self.mock_binbot_api.create_bot.return_value = BotResponse(
            message="Successfully created one bot.",
            data=BotModel(id=BOT_ID, pair="BTCUSDT", status=Status.inactive),
        )
        self.mock_binbot_api.activate_bot.return_value = BotResponse(
            message="Successfully activated bot.",
            data=BotModel(id=BOT_ID, pair="BTCUSDT", status=Status.active),
        )
        self.mock_binbot_api.submit_bot_event_logs.return_value = None
        self.mock_binbot_api.delete_bot.return_value = None
        self.mock_binbot_api.deactivate_bot.return_value = BotResponse(
            message="Successfully triggered panic sell! Bot deactivated.",
            data=BotModel(id=BOT_ID, pair="BTCUSDT", status=Status.completed),
        )
        self.mock_binbot_api.clean_margin_short.return_value = None
        self.mock_binbot_api.get_symbols.return_value = [
            SymbolModel(
                id="BTCUSDT",
                exchange_id=ExchangeId.BINANCE,
                base_asset="BTC",
                quote_asset="USDT",
                is_margin_trading_allowed=True,
            )
        ]
        # Methods used in KlinesProvider
        self.mock_binbot_api.get_autotrade_settings.return_value = self.settings
        self.mock_binbot_api.get_test_autotrade_settings.return_value = (
            self.test_settings
        )
        self.mock_binbot_api.get_market_breadth.return_value = []
        self.mock_binbot_api.get_symbols.return_value = [
            SymbolModel(
                id="BTCUSDT",
                exchange_id=ExchangeId.BINANCE,
                base_asset="BTC",
                quote_asset="USDT",
                is_margin_trading_allowed=True,
            )
        ]
        with patch("consumers.autotrade_consumer.KucoinFutures") as futures_cls:
            futures_cls.return_value.DEFAULT_MULTIPLIER = 1
            futures_cls.return_value.get_symbol_info.return_value = SimpleNamespace(
                multiplier=1,
                lot_size=1,
                taker_fee_rate=0,
            )
            self.consumer = AutotradeConsumer(
                autotrade_settings=self.settings,
                active_test_bots=[],
                all_symbols=[],
                test_autotrade_settings=self.test_settings,
                active_grid_ladders=[],
                binbot_api=self.mock_binbot_api,
            )

    def teardown_method(self):
        pass

    def _grid_params(
        self, symbol: str, generated_at: datetime | None = None
    ) -> GridDeploymentRequest:
        return GridDeploymentRequest(
            fiat="USDT",
            exchange=self.consumer.exchange,
            market_type=MarketType.FUTURES,
            algorithm_name="grid_ladder",
            generated_at=generated_at or datetime.now(UTC),
            symbol=symbol,
            range_low=95.0,
            range_high=105.0,
            level_count=7,
            total_margin=10.0,
            breakout_low=94.4,
            breakout_high=105.6,
            current_price=100.0,
            allocation_pct=50.0,
            cash_reserve_pct=25.0,
        )

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
    async def test_grid_only_policy_blocks_standard_real_bots(self):
        self.consumer.grid_only_policy = active_grid_only_policy()
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
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
            await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.get_available_fiat.assert_not_called()
        autotrade_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_grid_only_policy_does_not_block_grid_deployment(self):
        self.consumer.grid_only_policy = active_grid_only_policy()
        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_called_once()
        self.mock_binbot_api.create_grid_ladder.assert_called_once()

    @pytest.mark.asyncio
    async def test_grid_only_policy_still_allows_paper_trading(self):
        self.consumer.grid_only_policy = active_grid_only_policy()
        signal = SignalsConsumer(
            autotrade=False,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="paper_algo",
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

        autotrade_cls.assert_called_once_with(
            pair="BTCUSDT",
            settings=self.test_settings,
            algorithm_name="paper_algo",
            binbot_api=self.mock_binbot_api,
        )
        autotrade_instance.activate_autotrade.assert_awaited_once_with(signal)
        self.mock_binbot_api.get_available_fiat.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_skips_futures_when_minimum_margin_exceeds_balance(
        self,
    ):
        self.consumer.exchange = ExchangeId.KUCOIN
        self.mock_binbot_api.get_available_fiat.return_value = 15
        self.mock_binbot_api.get_single_symbol.return_value = SymbolModel(
            id="BTCUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="BTC",
            quote_asset="USDT",
            price_precision=2,
            qty_precision=0,
            is_margin_trading_allowed=True,
            futures_leverage=1,
        )
        signal = SignalsConsumer(
            autotrade=True,
            current_price=10,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="coinrule_buy_the_dip",
                market_type=MarketType.FUTURES,
                position=Position.short,
                fiat="USDT",
                fiat_order_size=10,
                stop_loss=1,
            ),
        )

        self.consumer.kucoin_futures_api.DEFAULT_MULTIPLIER = 1
        cast(
            Any, self.consumer.kucoin_futures_api.get_symbol_info
        ).return_value = SimpleNamespace(
            multiplier=10,
            lot_size=1,
            taker_fee_rate=0.0006,
        )

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            await self.consumer.process_autotrade_restrictions(signal)

        autotrade_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_allows_futures_when_margin_leaves_reversal_reserve(
        self,
    ):
        self.consumer.exchange = ExchangeId.KUCOIN
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        self.mock_binbot_api.get_single_symbol.return_value = SymbolModel(
            id="BTCUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="BTC",
            quote_asset="USDT",
            price_precision=2,
            qty_precision=0,
            is_margin_trading_allowed=True,
            futures_leverage=1,
        )
        signal = SignalsConsumer(
            autotrade=True,
            current_price=10,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="coinrule_buy_the_dip",
                market_type=MarketType.FUTURES,
                position=Position.short,
                fiat="USDT",
                fiat_order_size=200,
                stop_loss=1,
            ),
        )

        self.consumer.kucoin_futures_api.DEFAULT_MULTIPLIER = 1
        cast(
            Any, self.consumer.kucoin_futures_api.get_symbol_info
        ).return_value = SimpleNamespace(
            multiplier=10,
            lot_size=1,
            taker_fee_rate=0.0006,
        )

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            autotrade_instance = autotrade_cls.return_value
            autotrade_instance.activate_autotrade = AsyncMock()

            await self.consumer.process_autotrade_restrictions(signal)

        autotrade_cls.assert_called_once_with(
            pair="BTCUSDTM",
            settings=self.settings,
            algorithm_name="coinrule_buy_the_dip",
            db_collection_name="bots",
            binbot_api=self.mock_binbot_api,
        )
        autotrade_instance.activate_autotrade.assert_awaited_once_with(signal)
        assert signal.bot_params is not None
        assert signal.bot_params.fiat_order_size == 200

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_scales_futures_order_size_down(
        self,
    ):
        self.consumer.exchange = ExchangeId.KUCOIN
        self.mock_binbot_api.get_available_fiat.return_value = 60
        self.mock_binbot_api.get_single_symbol.return_value = SymbolModel(
            id="BTCUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="BTC",
            quote_asset="USDT",
            price_precision=2,
            qty_precision=0,
            is_margin_trading_allowed=True,
            futures_leverage=3,
        )
        signal = SignalsConsumer(
            autotrade=True,
            current_price=10,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="coinrule_buy_the_dip",
                market_type=MarketType.FUTURES,
                position=Position.short,
                fiat="USDT",
                fiat_order_size=500,
                stop_loss=1,
            ),
        )

        self.consumer.kucoin_futures_api.DEFAULT_MULTIPLIER = 1
        cast(
            Any, self.consumer.kucoin_futures_api.get_symbol_info
        ).return_value = SimpleNamespace(
            multiplier=0.001,
            lot_size=1,
            taker_fee_rate=0.0006,
        )

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            autotrade_instance = autotrade_cls.return_value
            autotrade_instance.activate_autotrade = AsyncMock()

            await self.consumer.process_autotrade_restrictions(signal)

        # min_step_margin = 1*10*0.001/3 + 2*0.01*0.0006 ≈ 0.003345
        # reversal_reserve ≈ 1.401, spendable ≈ 58.599
        # requested 500 > spendable → scaled to 58.599.
        autotrade_cls.assert_called_once()
        assert signal.bot_params is not None
        assert signal.bot_params.fiat_order_size is not None
        assert signal.bot_params.fiat_order_size < 500
        assert signal.bot_params.fiat_order_size > 0

    @pytest.mark.asyncio
    async def test_activate_autotrade_merges_signal_bot_params_over_settings(self):
        settings = make_autotrade_settings()
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
        assert create_payload["recovery_params"] is None
        assert create_payload["stop_loss"] == settings.stop_loss
        assert create_payload["take_profit"] == settings.take_profit
        assert create_payload["trailing_deviation"] == settings.trailing_deviation

    @pytest.mark.asyncio
    async def test_activation_error_deactivates_real_bot_without_deleting_it(self):
        settings = make_autotrade_settings(autoswitch=False)
        signal = SignalsConsumer(autotrade=True, current_price=100)
        self.mock_binbot_api.activate_bot.return_value = BotResponse(
            error=1,
            message="activation failed",
        )

        with patch("shared.autotrade.BinanceApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDT",
                settings=settings,
                algorithm_name="coinrule_buy_the_dip",
                db_collection_name="bots",
                binbot_api=self.mock_binbot_api,
            )

        with pytest.raises(AutotradeError, match="activation failed"):
            await autotrade.activate_autotrade(signal)

        self.mock_binbot_api.deactivate_bot.assert_called_once_with(
            BOT_ID,
            algorithmic_close=True,
        )
        self.mock_binbot_api.delete_bot.assert_not_called()

    @pytest.mark.asyncio
    async def test_activation_error_still_deletes_paper_bot(self):
        settings = make_autotrade_settings(autoswitch=False)
        signal = SignalsConsumer(autotrade=True, current_price=100)
        self.mock_binbot_api.activate_paper_bot.return_value = BotResponse(
            error=1,
            message="activation failed",
        )

        with patch("shared.autotrade.BinanceApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDT",
                settings=settings,
                algorithm_name="coinrule_buy_the_dip",
                db_collection_name="paper_trading",
                binbot_api=self.mock_binbot_api,
            )

        with pytest.raises(AutotradeError, match="activation failed"):
            await autotrade.activate_autotrade(signal)

        self.mock_binbot_api.delete_paper_bot.assert_called_once_with(BOT_ID)
        self.mock_binbot_api.deactivate_bot.assert_not_called()

    @pytest.mark.asyncio
    async def test_activate_autotrade_enables_bounded_recovery_for_autoswitch(self):
        settings = make_autotrade_settings(exchange_id=ExchangeId.KUCOIN)
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="spike_hunter_v3_kucoin",
                market_type=MarketType.FUTURES,
                position=Position.long,
            ),
        )

        with patch("shared.autotrade.KucoinApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDTM",
                settings=settings,
                algorithm_name="spike_hunter_v3_kucoin",
                db_collection_name="bots",
                binbot_api=self.mock_binbot_api,
            )

        await autotrade.activate_autotrade(signal)

        create_payload = self.mock_binbot_api.create_bot.call_args.args[0]
        assert create_payload["margin_short_reversal"] is True
        assert create_payload["recovery_params"] == {
            "reversal_path": "source",
            "source_contracts": 0,
            "source_loss_fiat": 0,
            "stop_loss_pct": 0,
        }

    @pytest.mark.asyncio
    async def test_activate_autotrade_preserves_explicit_recovery_opt_out(self):
        settings = make_autotrade_settings(exchange_id=ExchangeId.KUCOIN)
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="spike_hunter_v3_kucoin",
                market_type=MarketType.FUTURES,
                position=Position.long,
                recovery_params=None,
            ),
        )

        with patch("shared.autotrade.KucoinApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDTM",
                settings=settings,
                algorithm_name="spike_hunter_v3_kucoin",
                db_collection_name="bots",
                binbot_api=self.mock_binbot_api,
            )

        await autotrade.activate_autotrade(signal)

        create_payload = self.mock_binbot_api.create_bot.call_args.args[0]
        assert create_payload["margin_short_reversal"] is True
        assert create_payload["recovery_params"] is None

    @pytest.mark.asyncio
    async def test_activate_kucoin_futures_short_uses_mark_price_for_preflight(self):
        settings = make_autotrade_settings(exchange_id=ExchangeId.KUCOIN)
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="spike_hunter_v3_kucoin",
                market_type=MarketType.FUTURES,
                position=Position.short,
                fiat_order_size=10,
                stop_loss=3,
            ),
        )
        spot_api = MagicMock()
        futures_api = MagicMock()
        futures_api.get_mark_price.return_value = 100

        with (
            patch("shared.autotrade.KucoinApi", return_value=spot_api),
            patch("shared.autotrade.KucoinFutures", return_value=futures_api),
        ):
            autotrade = Autotrade(
                pair="BTCUSDTM",
                settings=settings,
                algorithm_name="spike_hunter_v3_kucoin",
                db_collection_name="bots",
                binbot_api=self.mock_binbot_api,
            )

            await autotrade.activate_autotrade(signal)

        futures_api.get_mark_price.assert_called_once_with("BTCUSDTM")
        spot_api.get_ticker_price.assert_not_called()
        create_payload = self.mock_binbot_api.create_bot.call_args.args[0]
        assert create_payload["market_type"] == "FUTURES"
        assert create_payload["position"] == "short"

    @pytest.mark.asyncio
    async def test_activate_kucoin_futures_short_raises_clear_error_without_mark_price(
        self,
    ):
        settings = make_autotrade_settings(exchange_id=ExchangeId.KUCOIN)
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="spike_hunter_v3_kucoin",
                market_type=MarketType.FUTURES,
                position=Position.short,
                fiat_order_size=10,
                stop_loss=3,
            ),
        )
        futures_api = MagicMock()
        futures_api.get_mark_price.side_effect = ValueError(
            "KuCoin futures mark price returned no value for BTCUSDTM"
        )

        with (
            patch("shared.autotrade.KucoinApi", return_value=MagicMock()),
            patch("shared.autotrade.KucoinFutures", return_value=futures_api),
        ):
            autotrade = Autotrade(
                pair="BTCUSDTM",
                settings=settings,
                algorithm_name="spike_hunter_v3_kucoin",
                db_collection_name="bots",
                binbot_api=self.mock_binbot_api,
            )

            with pytest.raises(
                ValueError,
                match="KuCoin futures mark price returned no value for BTCUSDTM",
            ):
                await autotrade.activate_autotrade(signal)

        self.mock_binbot_api.create_bot.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_skips_when_autotrade_false(self):
        signal = SignalsConsumer(
            autotrade=False,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )
        await self.consumer.process_autotrade_restrictions(signal)
        self.mock_binbot_api.create_grid_ladder.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_rejects_limit_and_duplicate(self):
        self.consumer.autotrade_settings.grid_max_active_ladders = 2
        self.mock_binbot_api.get_active_grid_ladders.return_value = [
            {"symbol": "BTCUSDT"},
            {"symbol": "ETHUSDT"},
            {"symbol": "SOLUSDT"},
        ]
        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("XRPUSDT"),
        )
        await self.consumer.process_autotrade_restrictions(signal)
        self.mock_binbot_api.create_grid_ladder.assert_not_called()

        self.mock_binbot_api.get_active_grid_ladders.return_value = [
            {"symbol": "BTCUSDT"}
        ]
        signal_dup = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )
        await self.consumer.process_autotrade_restrictions(signal_dup)
        self.mock_binbot_api.create_grid_ladder.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_rejects_duplicate_model_record(self):
        self.consumer.autotrade_settings.grid_max_active_ladders = 2
        self.mock_binbot_api.get_active_grid_ladders.return_value = [
            GridLadderRecord(
                symbol="BTCUSDT",
                fiat="USDT",
                exchange="kucoin",
                market_type="FUTURES",
                algorithm_name="grid_ladder",
                status=GridLadderStatus.pending,
                range_low=95,
                range_high=105,
                grid_step=5,
                level_count=3,
                total_margin=10,
                breakout_low=94,
                breakout_high=106,
            )
        ]
        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.create_grid_ladder.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_skips_when_active_bot_owns_symbol(self):
        self.mock_binbot_api.get_active_pairs.return_value = ["BTCUSDT"]

        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.create_grid_ladder.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_skips_when_grid_ladder_owns_symbol(
        self,
    ):
        self.consumer.exchange = ExchangeId.KUCOIN
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        self.mock_binbot_api.get_active_pairs.return_value = []
        self.mock_binbot_api.get_active_grid_ladders.return_value = [
            GridLadderRecord(
                symbol="BTCUSDTM",
                fiat="USDT",
                exchange="kucoin",
                market_type="FUTURES",
                algorithm_name="grid_ladder",
                status=GridLadderStatus.active,
                range_low=95,
                range_high=105,
                grid_step=5,
                level_count=3,
                total_margin=10,
                breakout_low=94,
                breakout_high=106,
            )
        ]
        self.mock_binbot_api.get_single_symbol.return_value = SymbolModel(
            id="BTCUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="BTC",
            quote_asset="USDT",
            price_precision=2,
            qty_precision=0,
            is_margin_trading_allowed=True,
            futures_leverage=1,
        )
        signal = SignalsConsumer(
            autotrade=True,
            current_price=10,
            bot_params=BotBase(
                pair="BTCUSDTM",
                name="coinrule_buy_the_dip",
                market_type=MarketType.FUTURES,
                position=Position.short,
                fiat="USDT",
                fiat_order_size=200,
                stop_loss=1,
            ),
        )

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            await self.consumer.process_autotrade_restrictions(signal)

        autotrade_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_calculates_before_create(self):
        """
        Success path: binquant asks binbot to calculate/validate levels before
        creating the ladder, and keeps the signal-emitted total_margin.
        """
        self.mock_binbot_api.get_active_grid_ladders.return_value = []

        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )
        assert signal.grid_params is not None
        signal.grid_params.total_margin = 1.0  # placeholder from LadderDeployer

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_called_once()
        self.mock_binbot_api.create_grid_ladder.assert_called_once()
        calculated_payload = self.mock_binbot_api.calculate_grid_levels.call_args.args[
            0
        ]
        payload = self.mock_binbot_api.create_grid_ladder.call_args.args[0]
        method_names = [
            method_call[0] for method_call in self.mock_binbot_api.method_calls
        ]
        assert calculated_payload["total_margin"] == 1.0
        assert payload["total_margin"] == 1.0
        assert signal.grid_params.total_margin == 1.0
        assert method_names.index("calculate_grid_levels") < method_names.index(
            "create_grid_ladder"
        )

    @pytest.mark.asyncio
    async def test_process_grid_deployment_calculate_400_skips_create_endpoint(
        self, caplog
    ):
        caplog.set_level("INFO")
        self.mock_binbot_api.get_active_grid_ladders.return_value = []
        self.mock_binbot_api.calculate_grid_levels.side_effect = BinbotErrors(
            "Grid level 0 cannot afford the exchange minimum contract size"
        )

        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_called_once()
        self.mock_binbot_api.create_grid_ladder.assert_not_called()
        assert self.consumer.grid_ladder_attempts == {}
        assert (
            "Grid level 0 cannot afford the exchange minimum contract size"
            in caplog.text
        )

    @pytest.mark.asyncio
    async def test_process_grid_deployment_does_not_cool_down_failed_calculate(self):
        self.mock_binbot_api.get_active_grid_ladders.return_value = []
        self.mock_binbot_api.calculate_grid_levels.side_effect = [
            BinbotErrors(
                "Grid level 0 cannot afford the exchange minimum contract size"
            ),
            None,
        ]
        generated_at = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)

        first_signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT", generated_at),
        )
        second_signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params(
                "BTCUSDT", generated_at + timedelta(minutes=15)
            ),
        )

        await self.consumer.process_autotrade_restrictions(first_signal)
        await self.consumer.process_autotrade_restrictions(second_signal)

        assert self.mock_binbot_api.calculate_grid_levels.call_count == 2
        self.mock_binbot_api.create_grid_ladder.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_skips_recent_create_attempt(self):
        self.mock_binbot_api.get_active_grid_ladders.return_value = []
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        generated_at = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)

        first_signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT", generated_at),
        )
        second_signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params(
                "BTCUSDT", generated_at + timedelta(minutes=15)
            ),
        )

        await self.consumer.process_autotrade_restrictions(first_signal)
        await self.consumer.process_autotrade_restrictions(second_signal)

        self.mock_binbot_api.create_grid_ladder.assert_called_once()
        self.mock_binbot_api.calculate_grid_levels.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_allows_attempt_after_cooldown(self):
        self.mock_binbot_api.get_active_grid_ladders.return_value = []
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        generated_at = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)

        first_signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT", generated_at),
        )
        second_signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params(
                "BTCUSDT", generated_at + timedelta(hours=1, minutes=1)
            ),
        )

        await self.consumer.process_autotrade_restrictions(first_signal)
        await self.consumer.process_autotrade_restrictions(second_signal)

        assert self.mock_binbot_api.create_grid_ladder.call_count == 2

    @pytest.mark.asyncio
    async def test_process_grid_deployment_swallows_create_grid_ladder_race(self):
        """
        Two binquant workers can both pass the active-ladder check between
        the GET and POST. The POST may then 400 against binbot's partial
        unique index — log and continue instead of bubbling out.
        """
        self.mock_binbot_api.get_active_grid_ladders.return_value = []
        self.mock_binbot_api.get_available_fiat.return_value = 1000
        self.mock_binbot_api.create_grid_ladder.side_effect = RuntimeError(
            "race: ladder already exists"
        )

        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        # Should not raise.
        await self.consumer.process_autotrade_restrictions(signal)
        self.mock_binbot_api.create_grid_ladder.assert_called_once()

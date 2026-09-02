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
    DealModel,
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


def active_grid_only_policy() -> GridOnlyPolicy:
    return GridOnlyPolicy.active(
        direction="toward_range",
        source="market_breadth_ma",
        latest=0.10,
        previous=0.12,
    )


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
            enable_grid_ladders=False,
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
        self.mock_binbot_api.get_bots_by_status.return_value = []
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

    def _closed_bot(
        self,
        *,
        position: Position = Position.long,
        market_type: MarketType = MarketType.SPOT,
        pair: str = "SCRUSDTM",
        opening_price: float,
        closing_price: float,
        opening_qty: float,
        total_commissions: float = 0.0,
        total_interests: float = 0.0,
    ) -> BotModel:
        return BotModel(
            id=BOT_ID,
            pair=pair,
            status=Status.completed,
            position=position,
            market_type=market_type,
            deal=DealModel(
                opening_price=opening_price,
                closing_price=closing_price,
                opening_qty=opening_qty,
                closing_qty=opening_qty,
                total_commissions=total_commissions,
                total_interests=total_interests,
            ),
        )

    def test_bot_realized_pnl_quote_matches_known_production_loss(self):
        # Validated against a real SCRUSDTM futures stop-loss fill: entry
        # 0.0212, close 0.0206, 583 contracts, 0.00247192 commissions ->
        # -0.3523. SCRUSDTM's real contract multiplier is 1.0 (mocked
        # below), matching setup_method's default kucoin_futures_api stub.
        bot = self._closed_bot(
            position=Position.long,
            market_type=MarketType.FUTURES,
            opening_price=0.0212,
            closing_price=0.0206,
            opening_qty=583,
            total_commissions=0.00247192,
        )
        assert self.consumer._bot_realized_pnl_quote(bot) == pytest.approx(
            -0.3523, abs=1e-4
        )

    def test_bot_realized_pnl_quote_short_direction_is_inverted(self):
        bot = self._closed_bot(
            position=Position.short,
            opening_price=100.0,
            closing_price=95.0,
            opening_qty=1.0,
        )
        assert self.consumer._bot_realized_pnl_quote(bot) == pytest.approx(5.0)

    def test_bot_realized_pnl_quote_applies_futures_contract_multiplier(self):
        # XBTUSDTM's real contract multiplier is 0.001 (1 contract = 0.001
        # BTC) — without applying it, this would be misstated by 1000x.
        cast(Any, self.consumer.kucoin_futures_api.get_symbol_info).side_effect = (
            lambda symbol: SimpleNamespace(
                multiplier=0.001, lot_size=1, taker_fee_rate=0
            )
        )
        bot = self._closed_bot(
            market_type=MarketType.FUTURES,
            pair="XBTUSDTM",
            opening_price=100_000.0,
            closing_price=101_000.0,
            opening_qty=10,
        )
        # price delta * qty * multiplier = 1000 * 10 * 0.001 = 10.0
        assert self.consumer._bot_realized_pnl_quote(bot) == pytest.approx(10.0)

    def test_bot_realized_pnl_quote_spot_bot_skips_multiplier_lookup(self):
        bot = self._closed_bot(
            market_type=MarketType.SPOT,
            opening_price=100.0,
            closing_price=101.0,
            opening_qty=1.0,
        )
        assert self.consumer._bot_realized_pnl_quote(bot) == pytest.approx(1.0)
        cast(Any, self.consumer.kucoin_futures_api.get_symbol_info).assert_not_called()

    def test_bot_realized_pnl_quote_excludes_bot_when_multiplier_lookup_fails(self):
        cast(
            Any, self.consumer.kucoin_futures_api.get_symbol_info
        ).side_effect = Exception("boom")
        bot = self._closed_bot(
            market_type=MarketType.FUTURES,
            pair="XBTUSDTM",
            opening_price=100_000.0,
            closing_price=90_000.0,
            opening_qty=10,
        )
        assert self.consumer._bot_realized_pnl_quote(bot) == pytest.approx(0.0)

    def test_estimated_daily_realized_pnl_sums_closed_bots(self):
        self.mock_binbot_api.get_bots_by_status.return_value = [
            self._closed_bot(opening_price=100.0, closing_price=99.0, opening_qty=1.0),
            self._closed_bot(opening_price=100.0, closing_price=101.0, opening_qty=1.0),
        ]
        assert self.consumer.estimated_daily_realized_pnl() == pytest.approx(0.0)

    def test_daily_loss_limit_not_reached_above_threshold(self):
        self.mock_binbot_api.get_bots_by_status.return_value = [
            self._closed_bot(opening_price=100.0, closing_price=99.501, opening_qty=1.0)
        ]
        assert not self.consumer.daily_loss_limit_reached()

    def test_daily_loss_limit_reached_at_threshold(self):
        self.mock_binbot_api.get_bots_by_status.return_value = [
            self._closed_bot(opening_price=100.0, closing_price=99.5, opening_qty=1.0)
        ]
        assert self.consumer.daily_loss_limit_reached()

    @pytest.mark.asyncio
    async def test_process_autotrade_restrictions_blocks_real_bots_on_daily_loss_limit(
        self,
    ):
        self.mock_binbot_api.get_bots_by_status.return_value = [
            self._closed_bot(opening_price=100.0, closing_price=90.0, opening_qty=1.0)
        ]
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

        autotrade_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_blocks_on_daily_loss_limit(self):
        self.settings.enable_grid_ladders = True
        self.mock_binbot_api.get_bots_by_status.return_value = [
            self._closed_bot(opening_price=100.0, closing_price=90.0, opening_qty=1.0)
        ]
        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_not_called()
        self.mock_binbot_api.create_grid_ladder.assert_not_called()

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
    @pytest.mark.parametrize(
        "algorithm_name",
        [
            "coinrule_buy_the_dip",
            "failed_spike_fade",
            "mean_reversion_fade",
            "liquidation_sweep_pump",
            "relative_strength_impulse_rider",
            "top_gainer_early_momentum",
            "activity_burst_pump",
        ],
    )
    async def test_standard_strategies_autotrade(self, algorithm_name):
        self.consumer.grid_only_policy = active_grid_only_policy()
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name=algorithm_name,
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
            settings=self.settings,
            algorithm_name=algorithm_name,
            db_collection_name="bots",
            binbot_api=self.mock_binbot_api,
        )
        autotrade_instance.activate_autotrade.assert_awaited_once_with(signal)

    @pytest.mark.asyncio
    async def test_grid_only_policy_blocks_momentum_when_grid_ladders_enabled(self):
        self.settings.enable_grid_ladders = True
        self.consumer.grid_only_policy = active_grid_only_policy()
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="top_gainer_early_momentum",
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
    @pytest.mark.parametrize("algorithm_name", ["failed_spike_fade"])
    async def test_grid_only_policy_allows_range_strategy_exceptions(
        self, algorithm_name
    ):
        self.settings.enable_grid_ladders = True
        self.consumer.grid_only_policy = active_grid_only_policy()
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name=algorithm_name,
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

        autotrade_instance.activate_autotrade.assert_awaited_once_with(signal)

    @pytest.mark.asyncio
    async def test_active_grid_ladder_still_blocks_standard_bot_on_same_symbol(self):
        """A symbol owned by an active grid ladder remains unavailable."""
        self.mock_binbot_api.get_active_grid_ladders.return_value = [
            {"symbol": "BTCUSDT", "market_type": "SPOT"}
        ]
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="failed_spike_fade",
                market_type=MarketType.SPOT,
                position=Position.long,
                fiat="USDT",
                fiat_order_size=25,
            ),
        )

        with patch("consumers.autotrade_consumer.Autotrade") as autotrade_cls:
            await self.consumer.process_autotrade_restrictions(signal)

        autotrade_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_disabled_price_tracker_cannot_activate_from_queued_signal(self):
        signal = SignalsConsumer(
            autotrade=True,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="coinrule_price_tracker",
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
    async def test_grid_deployment_is_processed(self):
        self.settings.enable_grid_ladders = True
        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_called_once()
        self.mock_binbot_api.create_grid_ladder.assert_called_once()

    @pytest.mark.asyncio
    async def test_grid_deployment_skips_when_grid_ladders_disabled(self):
        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_not_called()
        self.mock_binbot_api.create_grid_ladder.assert_not_called()

    @pytest.mark.asyncio
    async def test_paper_trading_activates(self):
        self.settings.enable_grid_ladders = True
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

    @pytest.mark.asyncio
    async def test_failed_spike_fade_shadow_signal_never_creates_real_bot(self):
        signal = SignalsConsumer(
            autotrade=False,
            current_price=100,
            bot_params=BotBase(
                pair="BTCUSDT",
                name="failed_spike_fade",
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
            algorithm_name="failed_spike_fade",
            binbot_api=self.mock_binbot_api,
        )
        autotrade_instance.activate_autotrade.assert_awaited_once_with(signal)

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
                signal_id=41,
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
        assert create_payload["signal_id"] == 41
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
                name="failed_spike_fade",
                market_type=MarketType.FUTURES,
                position=Position.long,
            ),
        )

        with patch("shared.autotrade.KucoinApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDTM",
                settings=settings,
                algorithm_name="failed_spike_fade",
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
                name="failed_spike_fade",
                market_type=MarketType.FUTURES,
                position=Position.long,
                recovery_params=None,
            ),
        )

        with patch("shared.autotrade.KucoinApi", return_value=MagicMock()):
            autotrade = Autotrade(
                pair="BTCUSDTM",
                settings=settings,
                algorithm_name="failed_spike_fade",
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
                name="failed_spike_fade",
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
                algorithm_name="failed_spike_fade",
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
                name="failed_spike_fade",
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
                algorithm_name="failed_spike_fade",
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
        self.settings.enable_grid_ladders = True
        signal = SignalsConsumer(
            autotrade=False,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )
        await self.consumer.process_autotrade_restrictions(signal)
        self.mock_binbot_api.create_grid_ladder.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_grid_deployment_rejects_limit_and_duplicate(self):
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
        self.mock_binbot_api.get_active_grid_ladders.return_value = []

        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )
        assert signal.grid_params is not None
        signal.grid_params.signal_id = 42
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
        assert payload["signal_id"] == 42
        assert signal.grid_params.total_margin == 1.0
        assert method_names.index("calculate_grid_levels") < method_names.index(
            "create_grid_ladder"
        )

    @pytest.mark.asyncio
    async def test_process_grid_deployment_skips_when_enable_grid_ladders_is_false(
        self, caplog
    ):
        """enable_grid_ladders is the master switch (mirrors GridLadderLifecycle's
        gate on the binbot side): off blocks ladder creation even with a
        fully valid, autotrade-approved grid_deploy signal."""
        caplog.set_level("INFO")
        self.settings.enable_grid_ladders = False
        self.mock_binbot_api.get_active_grid_ladders.return_value = []

        signal = SignalsConsumer(
            autotrade=True,
            signal_kind="grid_deploy",
            grid_params=self._grid_params("BTCUSDT"),
        )

        await self.consumer.process_autotrade_restrictions(signal)

        self.mock_binbot_api.calculate_grid_levels.assert_not_called()
        self.mock_binbot_api.create_grid_ladder.assert_not_called()
        assert "grid_ladder skipped: enable_grid_ladders_disabled" in caplog.text

    @pytest.mark.asyncio
    async def test_process_grid_deployment_calculate_400_skips_create_endpoint(
        self, caplog
    ):
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
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
        self.settings.enable_grid_ladders = True
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

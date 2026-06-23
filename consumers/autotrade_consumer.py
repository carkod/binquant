import logging
from datetime import UTC, datetime
from typing import Any

from pybinbot import (
    BinbotApi,
    BinbotErrors,
    BotBase,
    ExchangeId,
    GridDeploymentRequest,
    KucoinFutures,
    MarketType,
    SignalsConsumer,
    round_numbers,
    AutotradeSettingsSchema,
    TestAutotradeSettingsSchema,
)
from shared.autotrade import Autotrade
from shared.config import Config


class AutotradeConsumer:
    FUTURES_REVERSAL_BUFFER = 1.40
    GRID_DEPLOYMENT_ATTEMPT_COOLDOWN_SECONDS = 60 * 60

    def __init__(
        self,
        autotrade_settings: AutotradeSettingsSchema,
        active_test_bots,
        all_symbols,
        test_autotrade_settings: TestAutotradeSettingsSchema,
        active_grid_ladders,
        binbot_api: BinbotApi,
    ) -> None:
        self.market_domination_reversal = False
        self.active_bots: list = []
        self.active_grid_ladders = active_grid_ladders
        self.paper_trading_active_bots: list = []
        self.active_test_bots: list = active_test_bots
        self.grid_ladder_attempts: dict[tuple[str, str, str, str], float] = {}
        # Because market domination analysis 40 weight from binance endpoints
        self.btc_change_perc = 0
        self.volatility = 0

        # API dependencies
        self.autotrade_settings = autotrade_settings
        self.all_symbols = all_symbols
        self.test_autotrade_settings = test_autotrade_settings
        self.exchange = autotrade_settings.exchange_id
        self.config = Config()
        self.binbot_api = binbot_api
        self.kucoin_futures_api = KucoinFutures(
            key=self.config.kucoin_key,
            secret=self.config.kucoin_secret,
            passphrase=self.config.kucoin_passphrase,
        )

    @staticmethod
    def _signal_value(bot_params: BotBase, field_name: str, fallback):
        if field_name in bot_params.model_fields_set:
            value = getattr(bot_params, field_name)
            if value is not None:
                return value

        return fallback

    @staticmethod
    def _required_margin_for_contracts(
        contracts: float,
        price: float,
        multiplier: float,
        futures_leverage: float,
        taker_fee_rate: float,
    ) -> float:
        if contracts <= 0 or price <= 0:
            return 0.0

        notional = contracts * price * multiplier
        initial_margin = notional / futures_leverage
        fees = 2 * notional * taker_fee_rate
        return round_numbers(initial_margin + fees, 8)

    def _resolve_futures_order_size(
        self,
        *,
        symbol: str,
        price: float,
        stop_loss: float,
        fiat_order_size: float,
        available_balance: float,
    ) -> float | None:
        """
        Resolve the effective futures fiat_order_size, interpreted as the
        target margin committed to the trade. Auto-scales down to fit
        available_balance after reserving margin for a reversal trade.

        Returns the effective fiat_order_size, or None to skip autotrade.
        Contract count is derived downstream from this margin and current
        price, so a higher price simply means fewer contracts at the same
        committed cash.
        """
        if price <= 0:
            logging.info(
                "Skipping futures autotrade margin check because signal price is missing."
            )
            return fiat_order_size

        if stop_loss <= 0:
            logging.info(
                "Skipping futures autotrade because stop loss is not configured."
            )
            return None

        symbol_info = self.binbot_api.get_single_symbol(symbol)
        futures_symbol_info = self.kucoin_futures_api.get_symbol_info(symbol)

        multiplier = float(futures_symbol_info.multiplier)
        lot_size = float(futures_symbol_info.lot_size)
        taker_fee_rate = float(futures_symbol_info.taker_fee_rate)
        futures_leverage = float(symbol_info["futures_leverage"])

        min_step_margin = self._required_margin_for_contracts(
            lot_size,
            price,
            multiplier,
            futures_leverage,
            taker_fee_rate,
        )
        if min_step_margin <= 0:
            logging.info(
                "Skipping futures autotrade because lot margin is non-positive."
            )
            return None

        reversal_reserve = min_step_margin + self.FUTURES_REVERSAL_BUFFER
        spendable_balance = available_balance - reversal_reserve
        if spendable_balance < min_step_margin:
            logging.info(
                "Not enough funds to autotrade futures bot. "
                "One-lot margin %s plus reversal reserve %s exceeds available balance %s.",
                min_step_margin,
                reversal_reserve,
                available_balance,
            )
            return None

        if fiat_order_size < min_step_margin:
            logging.info(
                "Skipping futures autotrade because requested fiat_order_size %s "
                "is below one-lot margin %s for %s.",
                fiat_order_size,
                min_step_margin,
                symbol,
            )
            return None

        effective_margin = min(fiat_order_size, spendable_balance)

        if effective_margin < fiat_order_size:
            logging.info(
                "Scaling futures fiat_order_size from %s to %s to fit available balance %s.",
                fiat_order_size,
                effective_margin,
                available_balance,
            )

        return round_numbers(effective_margin, 8)

    def reached_max_active_autobots(self, db_collection_name: str) -> bool:
        """
        Check max `max_active_autotrade_bots` in controller settings

        Args:
        - db_collection_name: Database collection name ["paper_trading", "bots"]

        If total active bots > settings.max_active_autotrade_bots
        do not open more bots. There are two reasons for this:
        - In the case of test bots, infininately opening bots will open hundreds of bots
        which will drain memory and downgrade server performance
        - In the case of real bots, opening too many bots could drain all funds
        in bots that are actually not useful or not profitable. Some funds
        need to be left for Safety orders
        """
        if db_collection_name == "paper_trading":
            self.active_test_bots = self.binbot_api.get_active_pairs(
                collection_name="paper_trading"
            )
            active_count = len(self.active_test_bots)
            if active_count > self.test_autotrade_settings.max_active_autotrade_bots:
                return True

        if db_collection_name == "bots":
            self.active_bots = self.binbot_api.get_active_pairs(collection_name="bots")
            active_count = len(self.active_bots)
            if active_count > self.autotrade_settings.max_active_autotrade_bots:
                return True

        return False

    def is_margin_available(self, symbol: str) -> bool:
        """
        Check if margin trading is allowed for a symbol
        """
        is_margin_allowed = next(
            (
                item["is_margin_trading_allowed"]
                for item in self.all_symbols
                if item["id"] == symbol
            ),
            False,
        )
        return is_margin_allowed

    @staticmethod
    def _record_value(record: Any, field_name: str) -> Any:
        if isinstance(record, dict):
            return record.get(field_name)
        return getattr(record, field_name, None)

    def _has_active_grid_ladder(
        self, symbol: str, market_type: MarketType | str | None = None
    ) -> bool:
        self.active_grid_ladders = self.binbot_api.get_active_grid_ladders()
        for ladder in self.active_grid_ladders:
            if self._record_value(ladder, "symbol") != symbol:
                continue
            ladder_market_type = self._record_value(ladder, "market_type")
            if market_type is None or ladder_market_type is None:
                return True
            if MarketType(ladder_market_type) == MarketType(market_type):
                return True
        return False

    @staticmethod
    def _ratio_config(value: Any) -> float:
        parsed = float(value)
        return parsed / 100 if parsed > 1 else parsed

    @staticmethod
    def _grid_ladder_attempt_key(
        params: GridDeploymentRequest,
    ) -> tuple[str, str, str, str]:
        return (
            ExchangeId(params.exchange).value,
            MarketType(params.market_type).value,
            params.symbol,
            params.algorithm_name,
        )

    @staticmethod
    def _grid_ladder_attempt_timestamp(params: GridDeploymentRequest) -> float:
        generated_at = params.generated_at
        if not isinstance(generated_at, datetime):
            return datetime.now(UTC).timestamp()
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=UTC)
        return generated_at.timestamp()

    def _grid_ladder_attempted_recently(self, params: GridDeploymentRequest) -> bool:
        key = self._grid_ladder_attempt_key(params)
        attempt_ts = self._grid_ladder_attempt_timestamp(params)
        last_attempt_ts = self.grid_ladder_attempts.get(key)
        if last_attempt_ts is None:
            return False

        elapsed = attempt_ts - last_attempt_ts
        if 0 <= elapsed < self.GRID_DEPLOYMENT_ATTEMPT_COOLDOWN_SECONDS:
            logging.info(
                "grid_ladder skipped: recent create attempt for %s within %ss",
                params.symbol,
                self.GRID_DEPLOYMENT_ATTEMPT_COOLDOWN_SECONDS,
            )
            return True

        return False

    def _record_grid_ladder_attempt(self, params: GridDeploymentRequest) -> None:
        key = self._grid_ladder_attempt_key(params)
        self.grid_ladder_attempts[key] = self._grid_ladder_attempt_timestamp(params)

    async def process_grid_deployment(self, data: SignalsConsumer) -> None:
        params = data.grid_params
        autotrade = data.autotrade and self.autotrade_settings.autotrade
        if not params or not autotrade:
            logging.info("grid_ladder skipped: missing params or autotrade is false")
            return
        if self._grid_ladder_attempted_recently(params):
            return

        available_balance = float(
            self.binbot_api.get_available_fiat(
                exchange=self.exchange, fiat=self.autotrade_settings.fiat
            )
        )
        symbol = params.symbol
        self.active_bots = self.binbot_api.get_active_pairs(collection_name="bots")
        if symbol in self.active_bots:
            logging.info(
                "grid_ladder skipped: active production bot already owns %s", symbol
            )
            return

        self.active_grid_ladders = self.binbot_api.get_active_grid_ladders()
        grid_allocation_pct = params.allocation_pct
        cash_reserve_pct = params.cash_reserve_pct
        max_active = self.autotrade_settings.grid_max_active_ladders
        if (
            len(self.active_grid_ladders) >= max_active
            or any(
                self._record_value(ladder, "symbol") == symbol
                for ladder in self.active_grid_ladders
            )
            or grid_allocation_pct is None
            or cash_reserve_pct is None
        ):
            logging.info(
                "grid_ladder skipped: active_ladder_limit or symbol_already_active or missing_allocation_params"
            )
            return

        usable = available_balance * self._ratio_config(grid_allocation_pct)
        reserve = available_balance * self._ratio_config(cash_reserve_pct)
        per_ladder_cap = available_balance * self._ratio_config(
            self.autotrade_settings.max_margin_per_ladder_pct
        )
        deployable = max(usable - reserve, 0)
        remaining_slots = max_active - len(self.active_grid_ladders)
        suggested_margin = round_numbers(
            min(per_ladder_cap, deployable / remaining_slots), 8
        )
        if suggested_margin <= 0:
            logging.info("grid_ladder skipped: insufficient_available_balance")
            return
        # Mutate total_margin on the signal so downstream analytics
        # (dispatch_signal_record) sees the actual deployed amount, not the
        # candidate placeholder the strategy emitted.
        params.total_margin = suggested_margin
        payload = params.model_dump(mode="json")

        try:
            # Two binquant workers can both pass the active-ladder check between
            # the GET and POST, so the POST may 400 against binbot's partial
            # unique index. Log and move on instead of bubbling the exception
            # into the strategy pipeline.
            self._record_grid_ladder_attempt(params)
            self.binbot_api.create_grid_ladder(payload)
        except BinbotErrors as e:
            logging.info(e.message)
        except Exception:
            logging.exception(
                "create_grid_ladder failed for %s; another worker may have raced.",
                payload.get("symbol"),
            )

    async def process_autotrade_restrictions(self, result: SignalsConsumer):
        """
        Refactored autotrade conditions.
        Previously part of process_kline_stream

        1. Checks if we have balance to trade
        2. Check if we need to update websockets
        3. Check if autotrade is enabled
        4. Check if test algorithms (autotrade = False)
        5. Check active strategy
        """
        if result.signal_kind == "grid_deploy":
            await self.process_grid_deployment(result)
            return
        bot_params = result.bot_params
        if bot_params is None:
            logging.info(
                "Skipping autotrade processing because signal is missing bot_params."
            )
            return

        symbol = bot_params.pair
        algorithm_name = bot_params.name
        fiat = self._signal_value(bot_params, "fiat", self.autotrade_settings.fiat)
        requested_fiat_order_size = self._signal_value(
            bot_params,
            "fiat_order_size",
            self.autotrade_settings.base_order_size,
        )
        stop_loss = self._signal_value(
            bot_params, "stop_loss", self.autotrade_settings.stop_loss
        )
        market_type = MarketType(
            self._signal_value(bot_params, "market_type", MarketType.FUTURES)
        )

        # Includes both test and non-test autotrade
        # Test autotrade settings must be enabled
        if self.test_autotrade_settings.autotrade and not result.autotrade:
            if self.reached_max_active_autobots("paper_trading"):
                logging.info(
                    "Reached maximum number of paper_trading active bots set in controller settings"
                )
            elif symbol in self.active_test_bots:
                logging.info(
                    "Skipping paper trading: active bot already exists for %s", symbol
                )
            else:
                # Test autotrade runs independently of autotrade = 1
                test_autotrade = Autotrade(
                    pair=symbol,
                    settings=self.test_autotrade_settings,
                    algorithm_name=algorithm_name,
                    binbot_api=self.binbot_api,
                )
                await test_autotrade.activate_autotrade(result)

        # Check balance to avoid failed autotrades
        balance_check = self.binbot_api.get_available_fiat(
            exchange=self.exchange, fiat=fiat
        )
        if market_type != MarketType.FUTURES and balance_check < float(
            requested_fiat_order_size
        ):
            logging.info("Not enough funds to autotrade [bots].")
            return

        if (
            ExchangeId(self.exchange) == ExchangeId.KUCOIN
            and market_type == MarketType.FUTURES
        ):
            effective_fiat_order_size = self._resolve_futures_order_size(
                symbol=symbol,
                price=float(result.current_price),
                stop_loss=float(stop_loss),
                fiat_order_size=float(requested_fiat_order_size),
                available_balance=float(balance_check),
            )
            if effective_fiat_order_size is None:
                return
            # Propagate the (possibly scaled) margin to the bot via signal
            # overrides so downstream sizing matches what the gate approved.
            bot_params.fiat_order_size = effective_fiat_order_size

        """
        Real autotrade starts
        """
        if self.autotrade_settings.autotrade and result.autotrade:
            if self.reached_max_active_autobots("bots"):
                logging.info(
                    "Reached maximum number of active bots set in controller settings"
                )
            elif self._has_active_grid_ladder(symbol, market_type):
                logging.info(
                    "Skipping autotrade: active grid ladder already owns %s", symbol
                )
            elif symbol in self.active_bots:
                logging.info(
                    "Skipping autotrade: active bot already exists for %s", symbol
                )
            else:
                autotrade = Autotrade(
                    pair=symbol,
                    settings=self.autotrade_settings,
                    algorithm_name=algorithm_name,
                    db_collection_name="bots",
                    binbot_api=self.binbot_api,
                )
                await autotrade.activate_autotrade(result)

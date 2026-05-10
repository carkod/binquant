import logging

from pybinbot import (
    BinbotApi,
    BotBase,
    ExchangeId,
    KucoinFutures,
    MarketType,
    SignalsConsumer,
    round_numbers,
)
from shared.autotrade import Autotrade
from shared.config import Config


class AutotradeConsumer:
    FUTURES_REVERSAL_BUFFER = 1.40

    def __init__(
        self,
        autotrade_settings,
        active_test_bots,
        all_symbols,
        test_autotrade_settings,
        binbot_api: BinbotApi,
    ) -> None:
        self.market_domination_reversal = False
        self.active_bots: list = []
        self.paper_trading_active_bots: list = []
        self.active_bot_pairs: list = []
        self.active_test_bots: list = active_test_bots
        # Because market domination analysis 40 weight from binance endpoints
        self.btc_change_perc = 0
        self.volatility = 0

        # API dependencies
        self.autotrade_settings = autotrade_settings
        self.all_symbols = all_symbols
        self.test_autotrade_settings = test_autotrade_settings
        self.exchange = autotrade_settings["exchange_id"]
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

    def _has_required_futures_margin(
        self,
        *,
        symbol: str,
        price: float,
        stop_loss: float,
        fiat_order_size: float,
        available_balance: float,
    ) -> bool:
        if price <= 0:
            logging.info(
                "Skipping futures autotrade margin check because signal price is missing."
            )
            return True

        stop_loss_ratio = stop_loss / 100
        if stop_loss_ratio <= 0:
            logging.info(
                "Skipping futures autotrade because stop loss is not configured."
            )
            return False

        symbol_info = self.binbot_api.get_single_symbol(symbol)
        futures_symbol_info = self.kucoin_futures_api.get_symbol_info(symbol)

        multiplier = float(futures_symbol_info.multiplier)
        lot_size = float(futures_symbol_info.lot_size)
        taker_fee_rate = float(futures_symbol_info.taker_fee_rate)
        futures_leverage = float(symbol_info["futures_leverage"])
        qty_precision = int(symbol_info["qty_precision"])

        risk_sized_contracts = int(
            round_numbers(
                fiat_order_size / (stop_loss_ratio * price * multiplier),
                qty_precision,
            )
        )
        if risk_sized_contracts <= 0:
            logging.info(
                "Skipping futures autotrade because calculated contracts is 0."
            )
            return False

        min_step_margin = self._required_margin_for_contracts(
            lot_size,
            price,
            multiplier,
            futures_leverage,
            taker_fee_rate,
        )
        if min_step_margin > available_balance:
            logging.info(
                "Not enough funds to autotrade futures bot. "
                "Minimum contract margin %s exceeds available balance %s.",
                min_step_margin,
                available_balance,
            )
            return False

        reversal_reserve = min_step_margin + self.FUTURES_REVERSAL_BUFFER
        required_margin = self._required_margin_for_contracts(
            risk_sized_contracts,
            price,
            multiplier,
            futures_leverage,
            taker_fee_rate,
        )
        spendable_balance = available_balance - reversal_reserve

        if spendable_balance <= 0 or required_margin > spendable_balance:
            logging.info(
                "Not enough funds to autotrade futures bot. "
                "Required margin %s plus reversal reserve %s exceeds available balance %s.",
                required_margin,
                reversal_reserve,
                available_balance,
            )
            return False

        return True

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
            if active_count > self.test_autotrade_settings["max_active_autotrade_bots"]:
                return True

        if db_collection_name == "bots":
            self.active_bots = self.binbot_api.get_active_pairs(collection_name="bots")
            active_count = len(self.active_bots)
            if active_count > self.autotrade_settings["max_active_autotrade_bots"]:
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
        data = result
        bot_params = data.bot_params
        if bot_params is None:
            logging.info(
                "Skipping autotrade processing because signal is missing bot_params."
            )
            return

        symbol = bot_params.pair
        algorithm_name = bot_params.name
        fiat = self._signal_value(bot_params, "fiat", self.autotrade_settings["fiat"])
        requested_fiat_order_size = self._signal_value(
            bot_params,
            "fiat_order_size",
            self.autotrade_settings["base_order_size"],
        )
        stop_loss = self._signal_value(
            bot_params, "stop_loss", self.autotrade_settings["stop_loss"]
        )
        market_type = MarketType(
            self._signal_value(bot_params, "market_type", MarketType.FUTURES)
        )

        # Includes both test and non-test autotrade
        # Test autotrade settings must be enabled
        if (
            symbol not in self.active_test_bots
            and self.test_autotrade_settings["autotrade"]
            and not data.autotrade
        ):
            if self.reached_max_active_autobots("paper_trading"):
                logging.info(
                    "Reached maximum number of paper_trading active bots set in controller settings"
                )
            else:
                # Test autotrade runs independently of autotrade = 1
                test_autotrade = Autotrade(
                    pair=symbol,
                    settings=self.test_autotrade_settings,
                    algorithm_name=algorithm_name,
                    binbot_api=self.binbot_api,
                )
                await test_autotrade.activate_autotrade(data)

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
            and not self._has_required_futures_margin(
                symbol=symbol,
                price=float(data.current_price),
                stop_loss=float(stop_loss),
                fiat_order_size=float(requested_fiat_order_size),
                available_balance=float(balance_check),
            )
        ):
            return

        """
        Real autotrade starts
        """
        if (
            self.autotrade_settings["autotrade"]
            and data.autotrade
            and symbol not in self.active_bot_pairs
        ):
            if self.reached_max_active_autobots("bots"):
                logging.info(
                    "Reached maximum number of active bots set in controller settings"
                )
            else:
                autotrade = Autotrade(
                    pair=symbol,
                    settings=self.autotrade_settings,
                    algorithm_name=algorithm_name,
                    db_collection_name="bots",
                    binbot_api=self.binbot_api,
                )
                await autotrade.activate_autotrade(data)

        pass

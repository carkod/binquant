import logging

from pybinbot import SignalsConsumer, BinbotApi
from shared.autotrade import Autotrade


class AutotradeConsumer(BinbotApi):
    def __init__(
        self,
        autotrade_settings,
        active_test_bots,
        all_symbols,
        test_autotrade_settings,
    ) -> None:
        self.market_domination_reversal = False
        self.active_bots: list = []
        self.paper_trading_active_bots: list = []
        self.active_bot_pairs: list = []
        self.active_test_bots: list = active_test_bots
        self.all_symbols: list[dict] = []
        # Because market domination analysis 40 weight from binance endpoints
        self.btc_change_perc = 0
        self.volatility = 0

        # API dependencies
        self.autotrade_settings = autotrade_settings
        self.all_symbols = all_symbols
        self.test_autotrade_settings = test_autotrade_settings
        self.exchange = autotrade_settings["exchange_id"]
        self.binbot_api = BinbotApi()

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
        symbol = data.symbol

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
                    algorithm_name=data.algo,
                )
                await test_autotrade.activate_autotrade(data)

        # Check balance to avoid failed autotrades
        balance_check = self.get_available_fiat(
            exchange=self.exchange, fiat=self.autotrade_settings["fiat"]
        )
        if balance_check < float(self.autotrade_settings["base_order_size"]):
            logging.info("Not enough funds to autotrade [bots].")
            return

        """
        Real autotrade starts
        """
        if self.autotrade_settings["autotrade"] and data.autotrade:
            if self.reached_max_active_autobots("bots"):
                logging.info(
                    "Reached maximum number of active bots set in controller settings"
                )
            else:
                autotrade = Autotrade(
                    pair=symbol,
                    settings=self.autotrade_settings,
                    algorithm_name=data.algo,
                    db_collection_name="bots",
                )
                await autotrade.activate_autotrade(data)

        pass

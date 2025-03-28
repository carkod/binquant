import json
import logging

from models.signals import SignalsConsumer
from shared.apis import BinbotApi
from shared.autotrade import Autotrade


class AutotradeConsumer(BinbotApi):
    def __init__(self, producer) -> None:
        self.market_domination_reversal = False
        self.producer = producer
        self.active_bots: list = []
        self.paper_trading_active_bots: list = []
        self.active_bot_pairs: list = []
        self.active_test_bots: list = []
        self.all_symbols: list[dict] = []
        self.load_data_on_start()
        # Because market domination analysis 40 weight from binance endpoints
        self.btc_change_perc = 0
        self.volatility = 0
        pass

    def load_data_on_start(self):
        """
        Load data on start and on update_required
        """
        logging.info(
            "Loading controller, active bots and available symbols (not blacklisted)..."
        )
        self.autotrade_settings: dict = self.get_autotrade_settings()
        self.active_bot_pairs = self.get_active_pairs()
        self.paper_trading_active_bots = self.get_active_pairs(
            collection_name="paper_trading"
        )
        self.all_symbols = self.get_symbols()
        # Active bot symbols substracting exchange active symbols (not blacklisted)
        self.active_symbols = set({s["id"] for s in self.all_symbols}) - set(
            self.active_bot_pairs
        )
        self.active_test_bots = [
            item["pair"] for item in self.paper_trading_active_bots
        ]
        self.test_autotrade_settings: dict = self.get_test_autotrade_settings()
        pass

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
            active_count = len(self.active_test_bots)
            if active_count > self.test_autotrade_settings["max_active_autotrade_bots"]:
                return True

        if db_collection_name == "bots":
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

    def process_autotrade_restrictions(self, result: str):
        """
        Refactored autotrade conditions.
        Previously part of process_kline_stream

        1. Checks if we have balance to trade
        2. Check if we need to update websockets
        3. Check if autotrade is enabled
        4. Check if test autotrades
        5. Check active strategy
        """

        """
        Test autotrade starts

        Wrap in try and except to avoid bugs stopping real bot trades
        """
        payload = json.loads(result)
        data = SignalsConsumer(**payload)
        symbol = data.symbol

        # Skip testing algorithms
        if not data.autotrade:
            return

        if (
            symbol not in self.active_test_bots
            and self.test_autotrade_settings["autotrade"]
        ):
            if self.reached_max_active_autobots("paper_trading"):
                logging.info(
                    "Reached maximum number of active bots set in controller settings"
                )
            else:
                # Test autotrade runs independently of autotrade = 1
                test_autotrade = Autotrade(
                    symbol, self.test_autotrade_settings, data.algo, "paper_trading"
                )
                test_autotrade.activate_autotrade(data)

        # Check balance to avoid failed autotrades
        balance_check = self.get_available_fiat()
        if balance_check < float(self.autotrade_settings["base_order_size"]):
            logging.info("Not enough funds to autotrade [bots].")
            return

        """
        Real autotrade starts
        """
        if self.autotrade_settings["autotrade"] and symbol in self.active_symbols:
            if self.reached_max_active_autobots("bots"):
                logging.info(
                    "Reached maximum number of active bots set in controller settings"
                )
            else:
                if self.is_margin_available(symbol=symbol):
                    autotrade = Autotrade(
                        symbol, self.autotrade_settings, data.algo, "bots"
                    )
                    autotrade.activate_autotrade(data)

        return

import json
import logging

from models.signals import SignalsConsumer
from shared.apis.binbot_api import BinbotApi
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
        self.active_test_bots = self.get_active_pairs(collection_name="paper_trading")
        self.all_symbols = self.get_symbols()
        # Active bot symbols substracting exchange active symbols (not blacklisted)
        self.active_symbols = set(
            {s["id"] for s in self.all_symbols if s["active"]}
        ) - set(self.active_bot_pairs)
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

    async def process_autotrade_restrictions(self, result: str):
        """
        Refactored autotrade conditions.
        Previously part of process_kline_stream

        1. Checks if we have balance to trade
        2. Check if we need to update websockets
        3. Check if autotrade is enabled
        4. Check if test algorithms (autotrade = False)
        5. Check active strategy
        """
        payload = json.loads(result)
        data = SignalsConsumer(**payload)
        symbol = data.symbol

        logging.error(
            f"Autotrade consumer: {data.symbol} - {data.algo} - {data.autotrade}"
        )

        # Reload every time until fix restarting pipeline
        self.load_data_on_start()

        # Includes both test and non-test autotrade
        # Test autotrade settings must be enabled
        if (
            symbol not in self.active_test_bots
            and self.test_autotrade_settings["autotrade"]
            and not data.autotrade
        ):
            if self.reached_max_active_autobots("paper_trading"):
                logging.error(
                    "Reached maximum number of active bots set in controller settings"
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
        balance_check = self.get_available_fiat()
        if balance_check < float(self.autotrade_settings["base_order_size"]):
            logging.info("Not enough funds to autotrade [bots].")
            return

        """
        Real autotrade starts
        """
        if (
            self.autotrade_settings["autotrade"]
            and symbol in self.active_symbols
            and data.autotrade
        ):
            if self.reached_max_active_autobots("bots"):
                logging.error(
                    "Reached maximum number of active bots set in controller settings"
                )
            else:
                logging.error("Running real autotrade...")
                if self.is_margin_available(symbol=symbol):
                    autotrade = Autotrade(
                        pair=symbol,
                        settings=self.autotrade_settings,
                        algorithm_name=data.algo,
                        db_collection_name="bots",
                    )
                    await autotrade.activate_autotrade(data)

        return

import logging
from shared.apis import BinbotApi
from datetime import datetime

class AutotradeConsumer(BinbotApi):
    def __init__(self) -> None:
        self.blacklist_data = self.get_blacklist()
        self.autotrade_settings = self.get_autotrade_settings()
        self.market_domination_ts = datetime.now()
        self.market_domination_trend = None
        self.market_domination_reversal = None
        pass

    def process_autotrade_restrictions(
        self, symbol, algorithm, test_only=False, *args, **kwargs
    ):
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
        try:
            if (
                symbol not in self.active_test_bots
                and int(self.test_autotrade_settings["autotrade"]) == 1
            ):
                if self.reached_max_active_autobots("paper_trading"):
                    logging.info(
                        "Reached maximum number of active bots set in controller settings"
                    )
                else:
                    # Test autotrade runs independently of autotrade = 1
                    test_autotrade = Autotrade(
                        symbol, self.test_autotrade_settings, algorithm, "paper_trading"
                    )
                    test_autotrade.activate_autotrade(**kwargs)
        except Exception as error:
            print(error)
            pass

        # Check balance to avoid failed autotrades
        balance_check = self.balance_estimate()
        if balance_check < float(self.settings['base_order_size']):
            print(f"Not enough funds to autotrade [bots].")
            return

        """
        Real autotrade starts
        """
        if (int(self.settings["autotrade"]) == 1
            and not test_only):
            if self.reached_max_active_autobots("bots"):
                logging.info("Reached maximum number of active bots set in controller settings")
            else:

                autotrade = Autotrade(symbol, self.settings, algorithm, "bots")
                autotrade.activate_autotrade(**kwargs)

        return

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
            if not self.test_autotrade_settings:
                self.load_data()

            active_bots_res = requests.get(
                url=self.bb_test_bot_url, params={"status": "active"}
            )
            active_bots = handle_binance_errors(active_bots_res)
            active_count = len(active_bots["data"])
            if active_count > self.test_autotrade_settings["max_active_autotrade_bots"]:
                return True

        if db_collection_name == "bots":
            if not self.settings:
                self.load_data()

            active_bots_res = requests.get(
                url=self.bb_bot_url, params={"status": "active"}
            )
            active_bots = handle_binance_errors(active_bots_res)
            active_count = len(active_bots["data"])
            if active_count > self.settings["max_active_autotrade_bots"]:
                return True

        return False
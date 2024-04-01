import logging
from datetime import datetime, timedelta
from time import time
from typing import Literal

import requests
from shared.apis import BinbotApi
from shared.autotrade import Autotrade
from shared.telegram_bot import TelegramBot
from shared.utils import handle_binance_errors


class SignalsBase(BinbotApi):
    """
    Tools and functions that are shared by all signals
    """

    def __init__(self):
        self.markets_streams = None
        self.skipped_fiat_currencies = [
            "DOWN",
            "UP",
            "AUD",
        ]  # on top of blacklist
        self.telegram_bot = TelegramBot()
        self.max_request = 950  # Avoid HTTP 411 error by separating streams
        self.active_symbols = []
        self.active_test_bots = []
        self.blacklist_data = []
        self.test_autotrade_settings = {}
        self.settings = {}
        # Because market domination analysis 40 weight from binance endpoints
        self.market_domination_ts = datetime.now()
        self.market_domination_trend = None
        self.market_domination_reversal = None
        self.top_coins_gainers = []

        self.btc_change_perc = 0
        self.volatility = 0

    def send_telegram(self, msg):
        """
        Send message with telegram bot
        To avoid Conflict - duplicate Bot error
        /t command will still be available in telegram bot
        """
        if not hasattr(self.telegram_bot, "updater"):
            self.telegram_bot.run_bot()

        self.telegram_bot.send_msg(msg)
        return

    def blacklist_coin(self, pair, msg):
        res = requests.post(
            url=self.bb_blacklist_url, json={"pair": pair, "reason": msg}
        )
        result = handle_binance_errors(res)
        return result

    def ticker_24(self, symbol: str | None = None):
        """
        Weight 40 without symbol
        https://github.com/carkod/binbot/issues/438

        Using cache
        """
        url = self.ticker24_url
        params = {"symbol": symbol}

        res = requests.get(url=url, params=params)
        data = handle_binance_errors(res)
        return data

    def get_latest_btc_price(self):
        # Get 24hr last BTCUSDT
        btc_ticker_24 = self.ticker_24("BTCUSDT")
        self.btc_change_perc = float(btc_ticker_24["priceChangePercent"])
        return self.btc_change_perc

    def check_market_momentum(self, now: datetime) -> bool:
        """
        Check market momentum
        If market momentum is negative, then don't trade
        """
        if (
            # (now.hour == 9 and now.minute == 0)
            # or (now.hour == 9 and now.minute == 30)
            # or (now.hour == 10 and now.minute == 0)
            # or (now.hour == 16 and now.minute == 0)
            # or (now.hour == 16 and now.minute == 30)
            # or (now.hour == 17 and now.minute == 0)
            # or (now.hour == 12 and now.minute == 0)
            # or (now.hour == 12 and now.minute == 30)
            # or (now.hour == 13 and now.minute == 0)
            now.minute == 0
        ):
            return True

        return False

    def load_data(self):
        """
        Load controller data

        - Global settings for autotrade
        - Updated blacklist
        """
        logging.info("Loading controller and blacklist data...")
        if self.settings and self.test_autotrade_settings:
            logging.info("Settings and Test autotrade settings already loaded, skipping...")
            return

        settings_res = requests.get(url=f"{self.bb_autotrade_settings_url}")
        settings_data = handle_binance_errors(settings_res)
        blacklist_res = requests.get(url=f"{self.bb_blacklist_url}")
        blacklist_data = handle_binance_errors(blacklist_res)

        # Show webscket errors
        if "error" in (settings_data, blacklist_res) and (
            settings_data["error"] == 1 or blacklist_res["error"] == 1
        ):
            print(settings_data)

        # Remove restart flag, as we are already restarting
        if (
            "update_required" not in settings_data
            or settings_data["data"]["update_required"]
        ):
            settings_data["data"]["update_required"] = time()
            research_controller_res = requests.put(
                url=self.bb_autotrade_settings_url, json=settings_data["data"]
            )
            handle_binance_errors(research_controller_res)

        # Logic for autotrade
        research_controller_res = requests.get(url=self.bb_autotrade_settings_url)
        research_controller = handle_binance_errors(research_controller_res)
        self.settings = research_controller["data"]

        test_autotrade_settings = requests.get(url=f"{self.bb_test_autotrade_url}")
        test_autotrade = handle_binance_errors(test_autotrade_settings)
        self.test_autotrade_settings = test_autotrade["data"]

        self.settings = settings_data["data"]
        self.blacklist_data = blacklist_data["data"]
        self.max_request = int(self.settings["max_request"])

        # if autrotrade enabled and it's not an already active bot
        # this avoids running too many useless bots
        # Temporarily restricting to 1 bot for low funds
        bots_res = requests.get(
            url=self.bb_bot_url, params={"status": "active", "no_cooldown": True}
        )
        active_bots = handle_binance_errors(bots_res)["data"]
        self.active_symbols = [bot["pair"] for bot in active_bots]

        paper_trading_bots_res = requests.get(
            url=self.bb_test_bot_url, params={"status": "active", "no_cooldown": True}
        )
        paper_trading_bots = handle_binance_errors(paper_trading_bots_res)
        self.active_test_bots = [item["pair"] for item in paper_trading_bots["data"]]

        self.market_domination()
        pass

    def post_error(self, msg):
        res = requests.put(
            url=self.bb_autotrade_settings_url, json={"system_logs": msg}
        )
        handle_binance_errors(res)
        return

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

    def market_domination(self) -> Literal["gainers", "losers", None]:
        """
        Get data from gainers and losers endpoint to analyze market trends

        We want to know when it's more suitable to do long positions
        when it's more suitable to do short positions
        For now setting threshold to 70% i.e.
        if > 70% of assets in a given market (USDT) dominated by gainers
        if < 70% of assets in a given market dominated by losers
        Establish the timing
        """
        now = datetime.now()
        momentum = self.check_market_momentum(now)
        # momentum = True
        if (
            # now >= self.market_domination_ts
            momentum
        ):
            logging.info(
                f"Performing market domination analyses. Current trend: {self.market_domination_trend}"
            )
            data = self.get_market_domination_series()
            # reverse to make latest series more important
            data["data"]["gainers_count"].reverse()
            data["data"]["losers_count"].reverse()
            gainers_count = data["data"]["gainers_count"]
            losers_count = data["data"]["losers_count"]
            self.market_domination_trend = None
            if gainers_count[-1] > losers_count[-1]:
                self.market_domination_trend = "gainers"

                # Check reversal
                if gainers_count[-2] < losers_count[-2]:
                    # Positive reversal
                    self.market_domination_reversal = True

            else:
                self.market_domination_trend = "losers"

                if gainers_count[-2] > losers_count[-2]:
                    # Negative reversal
                    self.market_domination_reversal = False

            self.btc_change_perc = self.get_latest_btc_price()
            reversal_msg = ""
            if self.market_domination_reversal is not None:
                reversal_msg = f"{'Positive reversal' if self.market_domination_reversal else 'Negative reversal'}"

            logging.info(f"Current USDT market trend is: {reversal_msg}. BTC 24hr change: {self.btc_change_perc}")
            self.market_domination_ts = datetime.now() + timedelta(hours=1)
        pass

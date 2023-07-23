import math
import json
import logging

from datetime import datetime, timedelta
from logging import info
from time import sleep, time
from requests import get

import numpy
import pandas as pd
import requests
from algorithms.ma_candlestick_drop import ma_candlestick_drop
from algorithms.ma_candlestick_jump import ma_candlestick_jump
from shared.apis import BinbotApi
from shared.autotrade import process_autotrade_restrictions
from shared.streaming.socket_client import SpotWebsocketStreamClient
from scipy import stats
from shared.telegram_bot import TelegramBot
from shared.utils import handle_binance_errors, round_numbers
from typing import Literal

class SetupSignals(BinbotApi):
    """
    Tools and functions that are shared by all signals
    """
    def __init__(self):
        self.interval = "15m"
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

    def _send_msg(self, msg):
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

    def load_data(self):
        """
        Load controller data

        - Global settings for autotrade
        - Updated blacklist
        """
        info("Loading controller and blacklist data...")
        if self.settings and self.test_autotrade_settings:
            info("Settings and Test autotrade settings already loaded, skipping...")
            return

        settings_res = requests.get(url=f"{self.bb_autotrade_settings_url}")
        settings_data = handle_binance_errors(settings_res)
        blacklist_res = requests.get(url=f"{self.bb_blacklist_url}")
        blacklist_data = handle_binance_errors(blacklist_res)

        self.market_domination()

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
        self.interval = self.settings["candlestick_interval"]
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
        pass

    def post_error(self, msg):
        res = requests.put(
            url=self.bb_autotrade_settings_url, json={"system_logs": msg}
        )
        handle_binance_errors(res)
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
        if datetime.now() >= self.market_domination_ts:
            print(f"Performing market domination analyses. Current trend: {self.market_domination_trend}")
            res = get(url=self.bb_gainers_losers)
            data = handle_binance_errors(res)
            gainers = 0
            losers = 0
            for item in data["data"]:
                if float(item["priceChangePercent"]) > 0:
                    gainers += 1
                elif float(item["priceChangePercent"]) == 0:
                    continue
                else:
                    losers += 1

            total = gainers + losers
            perc_gainers = (gainers / total) * 100
            perc_losers = (losers / total) * 100

            if perc_gainers > 70:
                self.market_domination_trend = "gainers"

            if perc_losers > 70:
                self.market_domination_trend = "losers"

            print(
                f"[{datetime.now()}] Current USDT market trend is: {self.market_domination_trend}"
            )
            self._send_msg(
                f"[{datetime.now()}] Current USDT market #trend is dominated by {self.market_domination_trend}"
            )
            self.market_domination_ts = datetime.now() + timedelta(minutes=15)
        return


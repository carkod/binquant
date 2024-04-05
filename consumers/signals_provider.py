import logging
from datetime import datetime, timedelta
from time import time
from typing import Literal

import requests
from shared.apis import BinbotApi
from shared.autotrade import Autotrade
from shared.telegram_bot import TelegramBot
from shared.utils import handle_binance_errors


class SignalsProvider(BinbotApi):
    """
    Tools and functions that are shared by all signals
    """

    def __init__(self, producer):
        self.producer = producer
        self.skipped_fiat_currencies = [
            "DOWN",
            "UP",
            "AUD",
        ]  # on top of blacklist
        self.active_symbols = []
        self.active_test_bots = []
        self.test_autotrade_settings = self.get_test_autotrade_settings()
        self.autotrade_settings = self.get_autotrade_settings()
        # Because market domination analysis 40 weight from binance endpoints
        
        self.top_coins_gainers = []

        self.btc_change_perc = 0
        self.volatility = 0
    
    def active_symbols(self):
        """
        if autrotrade enabled and it's not an already active bot
        this avoids running too many useless bots
        Temporarily restricting to 1 bot for low funds
        """
        
        active_bots = self.get_bots_by_status()
        self.active_symbols = [bot["pair"] for bot in active_bots]
    
    def active_test_bots(self):
        """
        Get active test bots
        """
        paper_trading_active_bots = requests.get(
            url=self.bb_test_bot_url, params={"status": "active", "no_cooldown": True}
        )
        self.active_test_bots = [item["pair"] for item in paper_trading_active_bots]

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

        # Logic for autotrade
        research_controller_res = requests.get(url=self.bb_autotrade_settings_url)
        research_controller = handle_binance_errors(research_controller_res)
        self.settings = research_controller["data"]

        test_autotrade_settings = requests.get(url=f"{self.bb_test_autotrade_url}")
        test_autotrade = handle_binance_errors(test_autotrade_settings)
        self.test_autotrade_settings = test_autotrade["data"]

        pass

    


    

    
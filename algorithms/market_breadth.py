import os
from datetime import datetime
from typing import TYPE_CHECKING

from models.signals import HABollinguerSpread, SignalsConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class MarketBreadthAlgo:
    def __init__(self, cls: "CryptoAnalytics") -> None:
        self.ti = cls
        self.api = cls.api
        self.binbot_api = BinbotApi()
        self.current_market_dominance = MarketDominance.NEUTRAL
        self.btc_change_perc = 0
        self.autotrade = True
        self.market_breadth_data = cls.market_breadth_data
        self.predicted_market_breadth = None
        self.btc_correlation: float = 0

    def calculate_reversal(self) -> None:
        """
        Get data from gainers and losers endpoint to analyze market trends

        We want to know when it's more suitable to do long positions
        when it's more suitable to do short positions
        For now setting threshold to 70% i.e.
        if > 70% of assets in a given market (USDC) dominated by gainers
        if < 70% of assets in a given market dominated by losers
        Establish the timing
        """
        self.top_coins_gainers = [item["symbol"] for item in self.ti.top_gainers_day]

        # Check current market dominance
        if (
            self.market_breadth_data is not None
            and "adp" in self.market_breadth_data
            and len(self.market_breadth_data["adp"]) >= 3
        ):
            if (
                datetime.now().minute % 10 == 0 and datetime.now().second == 0
            ) or self.btc_change_perc == 0:
                self.btc_change_perc = self.binbot_api.get_latest_btc_price()

            if (
                self.market_breadth_data["adp"][-1] > 0
                and self.market_breadth_data["adp"][-2] > 0
                and self.market_breadth_data["adp"][-3] > 0
                # Is there a gainers reversal trend?
                and self.market_breadth_data["adp"][-4] < 0
            ):
                self.btc_correlation, self.btc_price = (
                    self.binbot_api.get_btc_correlation(symbol=self.ti.symbol)
                )

                if self.btc_correlation > 0 and self.btc_change_perc > 0:
                    # Update current market dominance
                    self.current_market_dominance = MarketDominance.GAINERS
                    self.bot_strategy = Strategy.long
                    self.autotrade = True

            if (
                self.market_breadth_data["adp"][-1] < 0
                and self.market_breadth_data["adp"][-2] < 0
                and self.market_breadth_data["adp"][-3] < 0
                and self.market_breadth_data["adp"][-4] > 0
            ):
                self.btc_correlation, self.btc_price = (
                    self.binbot_api.get_btc_correlation(symbol=self.ti.symbol)
                )
                if self.btc_correlation < 0 and self.btc_change_perc < 0:
                    self.current_market_dominance = MarketDominance.LOSERS
                    self.bot_strategy = Strategy.margin_short
                    self.autotrade = False

        else:
            self.current_market_dominance = MarketDominance.NEUTRAL

        return

    async def signal(
        self, close_price: float, bb_high: float, bb_low: float, bb_mid: float
    ):
        if not self.market_breadth_data:
            return

        # Reduce network calls
        self.calculate_reversal()

        adp_diff = (
            self.market_breadth_data["adp"][-1] - self.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.market_breadth_data["adp"][-2] - self.market_breadth_data["adp"][-3]
        )

        # We want to trade when the market is at its lowest point
        if (
            self.current_market_dominance == MarketDominance.LOSERS
            and adp_diff > 0
            and adp_diff_prev > 0
        ):
            algo = "market_breadth"
            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {close_price}
            - Strategy: {self.bot_strategy.value}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='https://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=self.autotrade,
                current_price=close_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=self.bot_strategy,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )

            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

import os
from datetime import datetime
from typing import TYPE_CHECKING

from algorithms.nbeats_market_breadth import NBeatsMarketBreadth
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class MarketBreadthAlgo:
    def __init__(
        self, cls: "TechnicalIndicators"
    ) -> None:
        self.ti = cls
        self.current_market_dominance = MarketDominance.NEUTRAL
        self.btc_change_perc = 0
        self.autotrade = True
        self.market_breadth_data = None
        self.predicted_market_breadth = None

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
                self.btc_change_perc = self.ti.binbot_api.get_latest_btc_price()

            if (
                self.market_breadth_data["adp"][-1] > 0
                and self.market_breadth_data["adp"][-2] > 0
                and self.market_breadth_data["adp"][-3] > 0
                # Is there a gainers reversal trend?
                and self.market_breadth_data["adp"][-4] < 0
            ):
                btc_correlation = self.ti.binbot_api.get_btc_correlation(
                    symbol=self.ti.symbol
                )

                if btc_correlation > 0 and self.btc_change_perc > 0:
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
                btc_correlation = self.ti.binbot_api.get_btc_correlation(
                    symbol=self.ti.symbol
                )
                if btc_correlation < 0 and self.btc_change_perc < 0:
                    self.current_market_dominance = MarketDominance.LOSERS
                    self.bot_strategy = Strategy.margin_short
                    self.autotrade = False

        else:
            self.current_market_dominance = MarketDominance.NEUTRAL

        return

    async def predict_market_breadth(self, close_price: float, bb_high: float, bb_low: float, bb_mid: float):
        """
        Predict market breadth using NBeatsMarketBreadth model.
        This method is called when the market breadth data is available.
        """
        if not self.market_breadth_data:
            return None

        nb_mb = NBeatsMarketBreadth(
            cls=self.ti,
            close_price=close_price,
            bb_high=bb_high,
            bb_low=bb_low,
            bb_mid=bb_mid,
        )
        self.predicted_market_breadth = await nb_mb.predict(self.market_breadth_data)

        return self.predicted_market_breadth

    async def signal(self, close_price: float, bb_high: float, bb_low: float, bb_mid: float):
        if not self.market_breadth_data or datetime.now().minute % 30 == 0:
            self.market_breadth_data = await self.ti.binbot_api.get_market_breadth()

        if not self.market_breadth_data:
            return

        # Reduce network calls
        self.calculate_reversal()
<<<<<<< HEAD
<<<<<<< HEAD
        await self.predict_market_breadth(close_price=close_price, bb_high=bb_high, bb_low=bb_low, bb_mid=bb_mid)
=======
        # await self.predict_market_breadth()
>>>>>>> 045c39d (Temporarily disable nbeats predictions)
=======
        await self.predict_market_breadth(close_price=close_price, bb_high=bb_high, bb_low=bb_low, bb_mid=bb_mid)
>>>>>>> 597698f (Fix Nbeats prediction with improved performance)

        predicted_advancers = False

        if (
            self.predicted_market_breadth is not None
            and float(self.predicted_market_breadth.iloc[-1]) > 0
            and float(self.predicted_market_breadth.iloc[-2]) > 0
            and float(self.predicted_market_breadth.iloc[-3]) > 0
        ):
            predicted_advancers = True

        if self.current_market_dominance == MarketDominance.GAINERS:
            algo = "market_domination_reversal"
            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {self.close_price}
            - Strategy: {self.bot_strategy.value}
            - Predicted market breadth confirmation: {"Yes" if predicted_advancers else "No"}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='https://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=self.autotrade,
                current_price=self.close_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=self.bot_strategy,
                bb_spreads=BollinguerSpread(
                    bb_high=self.bb_high,
                    bb_mid=self.bb_mid,
                    bb_low=self.bb_low,
                ),
            )

            await self.ti.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )

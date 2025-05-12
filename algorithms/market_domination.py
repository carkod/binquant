import os
from datetime import datetime
from typing import TYPE_CHECKING

from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class MarketDominationAlgo:
    def __init__(
        self, cls: "TechnicalIndicators", close_price, bb_high, bb_low, bb_mid
    ) -> None:
        self.ti = cls
        self.close_price = close_price
        self.bb_high = bb_high
        self.bb_low = bb_low
        self.bb_mid = bb_mid
        self.current_market_dominance = MarketDominance.NEUTRAL
        self.reversal = False
        self.market_domination_data = cls.market_domination_data
        self.btc_change_perc = 0
        self.autotrade = True

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
        # reverse to make latest series more important
        self.market_domination_data["gainers_count"].reverse()
        self.market_domination_data["losers_count"].reverse()
        gainers_count = self.market_domination_data["gainers_count"]
        losers_count = self.market_domination_data["losers_count"]
        # no self.market_domination_data from db
        if len(gainers_count) < 2 and len(losers_count) < 2:
            return

        # Proportion indicates whether trend is significant or not
        proportion = max(gainers_count[-1], losers_count[-1]) / (
            gainers_count[-1] + losers_count[-1]
        )

        # Check current market dominance
        if gainers_count[-1] > losers_count[-1]:
            # Update current market dominance
            self.current_market_dominance = MarketDominance.GAINERS

            # Now check reversal (tides turning?)
            if (
                gainers_count[-2] < losers_count[-2]
                # and gainers_count[-3] < losers_count[-3]
                # More than 60% it's way past reversal
            ):
                self.reversal = True
                self.bot_strategy = Strategy.long
                return

        if gainers_count[-1] < losers_count[-1]:
            self.current_market_dominance = MarketDominance.LOSERS

            if (
                gainers_count[-2] > losers_count[-2]
                # and (gainers_count[-3] > losers_count[-3])
                and proportion < 0.6
            ):
                # Negative reversal
                self.reversal = True
                self.bot_strategy = Strategy.margin_short
                # Testing only
                self.autotrade = False
                return

        self.reversal = False
        return

    async def market_domination_signal(self):
        if not self.market_domination_data or datetime.now().minute % 30 == 0:
            self.market_domination_data = (
                await self.ti.binbot_api.get_market_domination_series()
            )

        if not self.market_domination_data:
            return

        # Reduce network calls
        if datetime.now().minute % 10 == 0 and datetime.now().second == 0:
            if self.btc_change_perc == 0:
                self.btc_change_perc = self.ti.binbot_api.get_latest_btc_price()

            self.calculate_reversal()

            if (
                self.reversal
                and self.current_market_dominance != MarketDominance.NEUTRAL
            ):
                btc_correlation = self.ti.binbot_api.get_btc_correlation(
                    symbol=self.ti.symbol
                )
                if (
                    self.current_market_dominance == MarketDominance.GAINERS
                    and btc_correlation > 0
                    and self.btc_change_perc < 0
                ) or (
                    self.current_market_dominance == MarketDominance.LOSERS
                    and btc_correlation < 0
                    and self.btc_change_perc > 0
                ):
                    return
                else:
                    strategy = Strategy.long

                algo = "market_domination_reversal"
                msg = f"""
                - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - Current price: {self.close_price}
                - Strategy: {strategy}
                - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
                - <a href='https://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
                """

                value = SignalsConsumer(
                    autotrade=self.autotrade,
                    current_price=self.close_price,
                    msg=msg,
                    symbol=self.ti.symbol,
                    algo=algo,
                    bot_strategy=strategy,
                    bb_spreads=BollinguerSpread(
                        bb_high=self.bb_high,
                        bb_mid=self.bb_mid,
                        bb_low=self.bb_low,
                    ),
                )

                await self.ti.producer.send(
                    KafkaTopics.signals.value, value=value.model_dump_json()
                )

import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING

from pandas import DataFrame, Series

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
        self.data = self.ti.binbot_api.get_market_domination_series()
        self.msf = None
        self.reversal = False

    def time_gpt_forecast(self):
        """
        Forecasting using GPT-3
        """

        dates = self.data["dates"][-10:]
        gainers_count = self.data["gainers_count"][-10:]
        losers_count = self.data["losers_count"][-10:]
        forecast_df = DataFrame(
            {
                "dates": dates,
                "gainers_count": Series(gainers_count),
            }
        )
        forecast_df["unique_id"] = forecast_df.index
        df_x = DataFrame(
            {
                "dates": dates,
                "ex_1": losers_count,
            }
        )
        df_x["unique_id"] = df_x.index
        self.msf = self.ti.times_gpt_api.multiple_series_forecast(
            df=forecast_df, df_x=df_x
        )

        return self.msf

    def calculate_reversal(self) -> None:
        """
        Get data from gainers and losers endpoint to analyze market trends

        We want to know when it's more suitable to do long positions
        when it's more suitable to do short positions
        For now setting threshold to 70% i.e.
        if > 70% of assets in a given market (USDT) dominated by gainers
        if < 70% of assets in a given market dominated by losers
        Establish the timing
        """
        if datetime.now().minute == 0:
            logging.info(
                f"Performing market domination analyses. Current trend: {self.ti.current_market_dominance}"
            )

            top_gainers_day = self.ti.binbot_api.get_top_gainers()["data"]
            self.top_coins_gainers = [item["symbol"] for item in top_gainers_day]
            # reverse to make latest series more important
            self.data["gainers_count"].reverse()
            self.data["losers_count"].reverse()
            gainers_count = self.data["gainers_count"]
            losers_count = self.data["losers_count"]
            # no self.data from db
            if len(gainers_count) < 2 and len(losers_count) < 2:
                return

            # Proportion indicates whether trend is significant or not
            # to be replaced by TimesGPT if that works better
            proportion = max(gainers_count[-1], losers_count[-1]) / (
                gainers_count[-1] + losers_count[-1]
            )

            # Check reversal
            if gainers_count[-1] > losers_count[-1]:
                # Update current market dominance
                self.current_market_dominance = MarketDominance.GAINERS

                if (
                    gainers_count[-2] > losers_count[-2]
                    and gainers_count[-3] > losers_count[-3]
                    and proportion < 0.6
                ):
                    self.reversal = True
                    self.bot_strategy = Strategy.long

            if gainers_count[-1] < losers_count[-1]:
                self.current_market_dominance = MarketDominance.LOSERS

                if (
                    gainers_count[-2] < losers_count[-2]
                    and (gainers_count[-3] < losers_count[-3])
                    and proportion < 0.6
                ):
                    # Negative reversal
                    self.reversal = True
                    self.bot_strategy = Strategy.margin_short

    def market_domination_signal(self, btc_correlation):
        self.calculate_reversal()

        if self.reversal and self.current_market_dominance != MarketDominance.NEUTRAL:
            if (
                self.current_market_dominance == MarketDominance.GAINERS
                and btc_correlation > 0
            ) or (
                self.current_market_dominance == MarketDominance.LOSERS
                and btc_correlation < 0
            ):
                strategy = Strategy.margin_short
            else:
                strategy = Strategy.long

            algo = "reversal"
            msg = f"""
            - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {self.close_price}
            - Reversal: {self.reversal}
            - Strategy: {strategy}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='https://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
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

            self.ti.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            ).add_callback(self.ti.base_producer.on_send_success).add_errback(
                self.ti.base_producer.on_send_error
            )

    def time_gpt_market_domination(self, close_price, gainers_count, losers_count):
        """
        Same as market_domination_signal but using TimesGPT
        to forecast it this means we get ahead of the market_domination before it reverses.
        """

        # Due to 50 requests per month limit
        # run only once a day for testing
        if len(self.data["dates"]) > 51 and (
            datetime.now().hour == 9 and datetime.now().minute == 0
        ):
            self.msf = self.time_gpt_forecast()

            if self.msf:
                forecasted_gainers = self.msf["gainers_count"].values[0]
                total_count = gainers_count[-1:] + losers_count[-1:]
                forecasted_losers = total_count - forecasted_gainers

                if forecasted_gainers > forecasted_losers:
                    # Update current market dominance
                    self.ti.current_market_dominance = MarketDominance.GAINERS

                    if (
                        gainers_count[-1] > losers_count[-1]
                        and gainers_count[-2] > losers_count[-2]
                    ):
                        self.ti.reversal = True
                        self.ti.bot_strategy = Strategy.long

                    algo = "time_gpt_reversal"

                    msg = f"""
                    - [{os.getenv('ENV')}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                    - Current price: {close_price}
                    - Strategy: {self.ti.bot_strategy.value}
                    - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
                    - <a href='http://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
                    """

                    value = SignalsConsumer(
                        autotrade=False,
                        current_price=close_price,
                        msg=msg,
                        symbol=self.ti.symbol,
                        algo=algo,
                        bot_strategy=self.ti.bot_strategy,
                        bb_spreads=None,
                    )

                    self.ti.producer.send(
                        KafkaTopics.signals.value, value=value.model_dump_json()
                    ).add_callback(self.ti.base_producer.on_send_success).add_errback(
                        self.ti.base_producer.on_send_error
                    )

import logging
import os
from typing import TYPE_CHECKING

from algorithms.spikehunter_v1 import SpikeHunter
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import Strategy

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


FIXED_PRICE_THRESHOLD = 0.021
FIXED_VOLUME_THRESHOLD = 1.5
FIXED_MOMENTUM_THRESHOLD = 0.012
FIXED_RSI_OVERSOLD = 30
FIXED_WINDOW = 12


class SpikeHunterMeme(SpikeHunter):
    def __init__(self, cls: "CryptoAnalytics"):
        """
        Spike Hunter algorithm for meme coins.

        This is derived from standard Spike Hunter but using index memes
        """
        super().__init__(cls=cls)
        self.limit = 500

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        """
        Standard spike hunter algorithm that detects spikes with no confirmations.
        """
        last_spike = self.get_spikes()

        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )

        if not last_spike:
            logging.debug("No recent spike detected for breakout.")
            return

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if (
            self.ti.btc_correlation < 0
            and current_price > bb_high
            and self.ti.btc_price < 0
        ):
            algo = "memes_spike_hunter_breakout"
            autotrade = True

            msg = f"""
                - 🔥 [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - 📅 Time: {last_spike["timestamp"].strftime("%Y-%m-%d %H:%M")}
                - 📈 Price: +{last_spike["price_change_pct"]}
                - 📊 Volume: {last_spike["volume_ratio"]}x above average
                - BTC Correlation: {self.ti.btc_correlation:.2f}
                - Autotrade?: {"Yes" if autotrade else "No"}
                - ADP diff: {adp_diff:.2f} (prev: {adp_diff_prev:.2f})
                - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
                - <a href='http://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
                """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=current_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=Strategy.long,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.ti.telegram_consumer.send_signal(value.model_dump_json())
            await self.ti.at_consumer.process_autotrade_restrictions(value)

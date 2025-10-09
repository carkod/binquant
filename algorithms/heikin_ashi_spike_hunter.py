import logging
from os import getenv
from typing import TYPE_CHECKING

from algorithms.heikin_ashi import HeikinAshi
from algorithms.spikehunter_v1 import SpikeHunter
from algorithms.whale_signals import WhaleSignals
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import Strategy

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class HASpikeHunter(SpikeHunter):
    """
    SpikeHunter algo but using Heikin Ashi candles
    """

    def __init__(self, cls: "CryptoAnalytics"):
        cls.clean_df = HeikinAshi().get_heikin_ashi(cls.clean_df)
        self.whale = WhaleSignals(cls=cls)
        super().__init__(cls=cls)

    async def ha_spike_hunter(
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

        last_whale_signal = self.whale.get_whale_signals_current()

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if current_price > bb_high and not (self.df["number_of_trades"] < 5).any():
            if last_whale_signal and (
                last_whale_signal["large_orders_threshold"]
                or last_whale_signal["price_concentration_cross"]
                or last_whale_signal["whale_activity_surge"]
                or last_whale_signal["buying_pressure_low"]
            ):
                algo = "ha_spike_hunter"
                autotrade = False

                msg = f"""
                    - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                    - 📅: {last_spike["timestamp"].strftime("%Y-%m-%d %H:%M")}
                    - $: +{last_spike["price_change_pct"]}
                    - 📊 Volume: {last_spike["volume_ratio"]}x above average
                    - Signal type: {last_spike["spike_type"]}
                    - Number of trades: {last_spike["number_of_trades"]}
                    - ₿ Correlation: {self.ti.btc_correlation:.2f}
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

            return True

import logging
import os
from typing import TYPE_CHECKING

from pybinbot import HABollinguerSpread, SignalsConsumer, Strategy, round_numbers

from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class GridTrading:
    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.df_15m = cls.df_15m
        self.config = cls.config
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context

    async def signal(
        self,
        current_price,
        bb_high,
        bb_mid,
        bb_low,
    ):
        """
        Coinrule-inspired grid trading rule for range-bound markets.

        Best when the market is chopping sideways and price repeatedly
        oscillates around a stable band. The setup buys oversold dips inside
        the range and proposes scaling out 10% every +2% move while keeping
        residual exposure open.
        """
        context = self.latest_market_context
        if context is not None:
            is_vertical_market = context.market_stress_score >= 0.35
            is_downtrend_market = (
                context.advancers_ratio <= 0.45 or context.long_tailwind <= 0
            )
            if is_vertical_market or is_downtrend_market:
                return

        self.df_15m = self.ti.df_15m.copy()
        if len(self.df_15m) < 48:
            logging.warning(
                f"15m candles grid trading not enough data for symbol: {self.symbol}"
            )
            return

        required_cols = ["high", "low", "close", "rsi"]
        recent_window = self.df_15m.tail(48)
        if recent_window[required_cols].isnull().any().any():
            return

        recent_high = float(recent_window["high"].max())
        recent_low = float(recent_window["low"].min())
        recent_close_base = float(recent_window["close"].iloc[0])
        range_width = (
            (recent_high - recent_low) / recent_low if recent_low != 0 else 0.0
        )
        range_drift = (
            abs((current_price - recent_close_base) / recent_close_base)
            if recent_close_base != 0
            else 0.0
        )
        rsi_value = float(self.df_15m["rsi"].iloc[-1])
        bb_width = ((bb_high - bb_low) / bb_mid) if bb_mid else 0.0
        lower_band_position = (
            (current_price - bb_low) / (bb_high - bb_low) if bb_high != bb_low else 0.5
        )

        is_range_market = (
            range_width >= 0.015 and range_drift <= 0.03 and 0.01 <= bb_width <= 0.08
        )
        buy_zone = (
            rsi_value < 35 and current_price <= bb_mid and lower_band_position <= 0.4
        )
        reduce_zone = rsi_value > 65

        if not is_range_market or not buy_zone:
            return

        algo = "coinrule_grid_trading"
        bot_strategy = Strategy.long
        autotrade = False

        kucoin_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[0]
        terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[1]

        msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {current_price}
            - Strategy: {bot_strategy.value}
            - Market mode: Sideways / range-bound
            - RSI buy zone (&lt; 35): {round_numbers(rsi_value, 2)}
            - Range width (24 candles): {round_numbers(range_width * 100, 2)}%
            - Range drift (24 candles): {round_numbers(range_drift * 100, 2)}%
            - Bollinger width: {round_numbers(bb_width * 100, 2)}%
            - Grid action: Buy ladder into weakness below the mid-band
            - Scale-out plan: Sell 10% every +2% move while keeping position open
            - Reduce exposure trigger: {"Active" if reduce_zone else "Stand by until RSI > 65"}
            - {"Autotrade is disabled for testing" if not autotrade else "Autotrade is enabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            symbol=self.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            market_type=self.market_type,
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

import os
from typing import TYPE_CHECKING
from pybinbot import HABollinguerSpread, MarketType, SignalsConsumer

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


async def top_gainers_drop(
    cls: "ContextEvaluator",
    close_price,
    open_price,
    bb_high,
    bb_mid,
    bb_low,
):
    """
    From the list of USDT top gainers
    pick the first 4, expect them to drop at some point
    so create margin_short bot

    """
    if float(close_price) < float(open_price) and cls.symbol in cls.top_coins_gainers:
        algo = "top_gainers_drop"
        autotrade = cls.should_autotrade(cls.bot_strategy, True)

        msg = f"""
        - [{os.getenv("ENV")}] Top gainers's drop <strong>#{algo} algorithm</strong> #{cls.symbol}
        - Current price: {close_price}
        - Bollinguer bands spread: {(bb_high - bb_low) / bb_high}
        - Reversal? {"Yes" if cls.market_domination_reversal else "No"}
        - Market domination trend: {cls.current_market_dominance}
        - Strategy: {cls.bot_strategy}
        - Autotrade?: {"Yes" if autotrade else "No"}
        - https://www.binance.com/en/trade/{cls.symbol}
        - <a href='http://terminal.binbot.in/bots/new/{cls.symbol}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=close_price,
            msg=msg,
            symbol=cls.symbol,
            algo=algo,
            bot_strategy=cls.bot_strategy,
            market_type=MarketType.FUTURES,
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await cls.telegram_consumer.send_signal(msg)
        await cls.at_consumer.process_autotrade_restrictions(value)

    return

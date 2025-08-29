from os import getenv

from pandas import DataFrame

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from models.signals import BollinguerSpread, SignalsConsumer
from shared.enums import Strategy
from shared.utils import supress_notation


async def local_min_max(
    df: DataFrame,
    symbol: str,
    current_price: float,
    bb_high: float,
    bb_mid: float,
    bb_low: float,
    telegram: TelegramConsumer,
    at_consumer: AutotradeConsumer,
) -> DataFrame:
    """
    Calculate local min and max for the closing price
    """
    # Detect local minima
    min_price = df["low"].min()

    if min_price == current_price:
        algo = "local_min_max"
        autotrade = False

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{symbol}
            - 🔥 {symbol} has hit a new minimum {supress_notation(min_price)}!!
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='https://www.binance.com/en/trade/{symbol}'>Binance</a>
            - <a href='http://terminal.binbot.in/bots/new/{symbol}'>Dashboard trade</a>
            """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            msg=msg,
            symbol=symbol,
            algo=algo,
            bot_strategy=Strategy.long,
            bb_spreads=BollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        await telegram.send_signal(value)
        await at_consumer.process_autotrade_restrictions(value)

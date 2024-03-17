import logging
from datetime import datetime
from logging import info
from time import sleep, time
from typing import Literal

import pandas as pd
import requests
from algorithms.coinrule import buy_low_sell_high, fast_and_slow_macd
from algorithms.ma_candlestick import ma_candlestick_drop, ma_candlestick_jump
from algorithms.price_changes import price_rise_15
from algorithms.rally import rally_or_pullback
from algorithms.top_gainer_drop import top_gainers_drop
from scipy import stats
from shared.apis import BinbotApi
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.telegram_bot import TelegramBot
from shared.utils import handle_binance_errors, round_numbers
from websocket import WebSocketException

from outbound_data.signals_outbound import SignalsOutbound

logging.basicConfig(
    filename="./binbot-research.log",
    filemode="a",
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

if __name__ == "__main__":
    try:
        rs = SignalsOutbound()
        rs.start_stream()
    except Exception as error:
        logging.error(f"Hey ya normal exception: {error}")
        rs = SignalsOutbound()
        rs.start_stream()

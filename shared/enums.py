from enum import Enum
from time import time

from pydantic import BaseModel, field_validator


class EnumDefinitions:
    """
    Enums established by Binance API
    """

    symbol_status = (
        "PRE_TRADING",
        "TRADING",
        "POST_TRADING",
        "END_OF_DAY",
        "HALT",
        "AUCTION_MATCH",
        "BREAK",
    )
    symbol_type = "SPOT"
    order_status = [
        "NEW",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCELED",
        "REJECTED",
        "EXPIRED",
    ]
    chart_intervals = (
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    )
    rate_limit_intervals = ("SECOND", "MINUTE", "DAY")
    order_book_limits = ("5", "10", "20", "50", "100", "500", "1000", "5000")


class BinbotEnums:
    statuses = ("inactive", "active", "completed", "error", "archived")
    mode = ("manual", "autotrade")
    strategy = ("long", "short", "margin_long", "margin_short")


class Status(str, Enum):
    inactive = "inactive"
    active = "active"
    completed = "completed"
    error = "error"
    archived = "archived"


class Strategy(str, Enum):
    long = "long"
    margin_short = "margin_short"


class OrderType(str, Enum):
    limit = "LIMIT"
    market = "MARKET"
    stop_loss = "STOP_LOSS"
    stop_loss_limit = "STOP_LOSS_LIMIT"
    take_profit = "TAKE_PROFIT"
    take_profit_limit = "TAKE_PROFIT_LIMIT"
    limit_maker = "LIMIT_MAKER"


class TimeInForce(str, Enum):
    gtc = "GTC"
    ioc = "IOC"
    fok = "FOK"


class OrderSide(str, Enum):
    buy = "BUY"
    sell = "SELL"


class CloseConditions(str, Enum):
    dynamic_trailling = "dynamic_trailling"
    timestamp = "timestamp"  # No trailling, standard stop loss
    market_reversal = (
        "market_reversal"  # binbot-research param (self.market_trend_reversal)
    )


class KafkaTopics(str, Enum):
    klines_store_topic = "klines_store_topic"
    processed_klines_topic = "processed_klines_topic"
    technical_indicators = "technical_indicators"
    signals = "signals"
    restart_streaming = "restart_streaming"


class BinanceKlineIntervals(str, Enum):
    one_minute = "1m"
    three_minutes = "3m"
    five_minutes = "5m"
    fifteen_minutes = "15m"
    thirty_minutes = "30m"
    one_hour = "1h"
    two_hours = "2h"
    four_hours = "4h"
    six_hours = "6h"
    eight_hours = "8h"
    twelve_hours = "12h"
    one_day = "1d"
    three_days = "3d"
    one_week = "1w"
    one_month = "1M"

    def bin_size(self):
        return int(self.value[:-1])

    def unit(self):
        if self.value[-1:] == "m":
            return "minute"
        elif self.value[-1:] == "h":
            return "hour"
        elif self.value[-1:] == "d":
            return "day"
        elif self.value[-1:] == "w":
            return "week"
        elif self.value[-1:] == "M":
            return "month"


class DealType(str, Enum):
    base_order = "base_order"
    take_profit = "take_profit"
    stop_loss = "stop_loss"
    short_sell = "short_sell"
    short_buy = "short_buy"
    margin_short = "margin_short"
    panic_close = "panic_close"


class BinanceOrderModel(BaseModel):
    """
    Data model given by Binance,
    therefore it should be strings
    """

    order_type: str
    time_in_force: str
    timestamp: int
    order_id: int
    order_side: str
    pair: str
    qty: float
    status: str
    price: float
    deal_type: DealType

    @field_validator("timestamp", "order_id", "price", "qty", "order_id")
    @classmethod
    def validate_str_numbers(cls, v):
        if isinstance(v, float):
            return v
        elif isinstance(v, int):
            return v
        elif isinstance(v, str):
            return float(v)
        else:
            raise ValueError(f"{v} must be a number")


class DealModel(BaseModel):
    """
    Data model that is used for operations,
    so it should all be numbers (int or float)
    """

    buy_price: float = 0
    buy_total_qty: float = 0
    buy_timestamp: float = time() * 1000
    current_price: float = 0
    sd: float = 0
    avg_buy_price: float = 0
    take_profit_price: float = 0
    sell_timestamp: float = 0
    sell_price: float = 0
    sell_qty: float = 0
    trailling_stop_loss_price: float = 0
    # take_profit but for trailling, to avoid confusion, trailling_profit_price always be > trailling_stop_loss_price
    trailling_profit_price: float = 0
    stop_loss_price: float = 0
    trailling_profit: float = 0
    so_prices: float = 0
    post_closure_current_price: float = 0
    original_buy_price: float = 0  # historical buy_price after so trigger
    short_sell_price: float = 0
    short_sell_qty: float = 0
    short_sell_timestamp: float = time() * 1000

    # fields for margin trading
    margin_short_loan_principal: float = 0
    margin_loan_id: float = 0
    hourly_interest_rate: float = 0
    margin_short_sell_price: float = 0
    margin_short_loan_interest: float = 0
    margin_short_buy_back_price: float = 0
    margin_short_sell_qty: float = 0
    margin_short_buy_back_timestamp: int = 0
    margin_short_base_order: float = 0
    margin_short_sell_timestamp: int = 0
    margin_short_loan_timestamp: int = 0

    @field_validator(
        "buy_price",
        "current_price",
        "avg_buy_price",
        "original_buy_price",
        "take_profit_price",
        "sell_price",
        "short_sell_price",
        "trailling_stop_loss_price",
        "trailling_profit_price",
        "stop_loss_price",
        "trailling_profit",
        "margin_short_loan_principal",
        "margin_short_sell_price",
        "margin_short_loan_interest",
        "margin_short_buy_back_price",
        "margin_short_base_order",
        "margin_short_sell_qty",
    )
    @classmethod
    def check_prices(cls, v):
        if float(v) < 0:
            raise ValueError("Price must be a positive number")
        elif isinstance(v, str):
            return float(v)
        return v

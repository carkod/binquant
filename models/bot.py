from time import time
from typing import Literal

from bson.objectid import ObjectId
from pydantic import BaseModel, Field, field_validator



class OrderSchema(BaseModel):
    order_type: str | None = None
    time_in_force: str | None = None
    timestamp: float = 0
    pair: str | None = None
    qty: str | None = None
    order_side: str | None = None
    order_id: str | None = None
    fills: str = None
    price: float | None = None
    status: str | None = None
    deal_type: str | None = None  # [base_order, take_profit, so_{x}, short_sell, short_buy, margin_short]


class DealSchema(BaseModel):
    buy_price: float = 0  # base currency quantity e.g. 3000 USDT in BTCUSDT
    base_order_price: float = 0 # To replace buy_price - better naming for both long and short positions
    buy_timestamp: float = 0
    buy_total_qty: float = 0
    current_price: float = 0
    sd: float = 0
    avg_buy_price: float = 0  # depricated - replaced with buy_price
    original_buy_price: float = 0  # historical buy_price after so executed. avg_buy_price = buy_price
    take_profit_price: float = 0  # quote currency quantity e.g. 0.00003 BTC in BTCUSDT (sell price)
    so_prices: float = 0
    sell_timestamp: float = 0
    sell_price: float = 0
    sell_qty: float = 0
    post_closure_current_price: float = 0
    trailling_stop_loss_price: float = 0
    trailling_profit_price: float = 0
    short_sell_price: float = 0
    short_sell_qty: float = 0
    short_sell_timestamp: float = 0
    stop_loss_price: float = 0
    margin_short_base_order: float = 0  # borrowed amount
    margin_loan_id: str = ""
    margin_short_loan_interest: float = 0
    margin_short_loan_principal: float = 0
    margin_short_loan_timestamp: float = 0
    margin_short_repay_price: float = 0
    margin_short_sell_price: float = 0
    margin_short_sell_timestamp: float = 0
    margin_short_buy_back_price: float = 0
    margin_short_buy_back_timestamp: float = 0
    hourly_interest_rate: float = 0

    @field_validator(
        "buy_price",
        "current_price",
        "avg_buy_price",
        "original_buy_price",
        "take_profit_price",
        "sell_price",
        "short_sell_price",
    )
    @classmethod
    def check_prices(cls, v):
        if float(v) < 0:
            raise ValueError("must be a positive number")
        return v


class MarginOrderSchema(OrderSchema):
    margin_buy_borrow_amount: int = 0
    margin_buy_borrow_asset: str = "USDT"
    is_isolated: bool = False


class DealSchema(BaseModel):
    buy_price: float = 0  # base currency quantity e.g. 3000 USDT in BTCUSDT
    base_order_price: float = 0 # To replace buy_price - better naming for both long and short positions
    buy_timestamp: float = 0
    buy_total_qty: float = 0
    current_price: float = 0
    sd: float = 0
    avg_buy_price: float = 0  # depricated - replaced with buy_price
    original_buy_price: float = 0  # historical buy_price after so executed. avg_buy_price = buy_price
    take_profit_price: float = 0  # quote currency quantity e.g. 0.00003 BTC in BTCUSDT (sell price)
    so_prices: float = 0
    sell_timestamp: float = 0
    sell_price: float = 0
    sell_qty: float = 0
    post_closure_current_price: float = 0
    trailling_stop_loss_price: float = 0
    trailling_profit_price: float = 0
    short_sell_price: float = 0
    short_sell_qty: float = 0
    short_sell_timestamp: float = 0
    stop_loss_price: float = 0
    margin_short_base_order: float = 0  # borrowed amount
    margin_loan_id: str = ""
    margin_short_loan_interest: float = 0
    margin_short_loan_principal: float = 0
    margin_short_loan_timestamp: float = 0
    margin_short_repay_price: float = 0
    margin_short_sell_price: float = 0
    margin_short_sell_timestamp: float = 0
    margin_short_buy_back_price: float = 0
    margin_short_buy_back_timestamp: float = 0
    hourly_interest_rate: float = 0

    @field_validator(
        "buy_price",
        "current_price",
        "avg_buy_price",
        "original_buy_price",
        "take_profit_price",
        "sell_price",
        "short_sell_price",
    )
    @classmethod
    def check_prices(cls, v):
        if float(v) < 0:
            raise ValueError("must be a positive number")
        return v

class SafetyOrderSchema(BaseModel):
    name: str = "so_1"  # should be so_<index>
    status: Literal[
        0, 1, 2
    ] = 0  # 0 = standby, safety order hasn't triggered, 1 = filled safety order triggered, 2 = error
    order_id: str | None = None
    created_at: float = time() * 1000
    updated_at: float = time() * 1000
    buy_price: float = 0  # base currency quantity e.g. 3000 USDT in BTCUSDT
    buy_timestamp: float = 0
    so_size: float = 0  # quote currency quantity e.g. 0.00003 BTC in BTCUSDT
    max_active_so: float = 0
    so_volume_scale: float = 0
    so_step_scale: float = 0
    so_asset: str = "USDT"
    errors: list[str] = []
    total_commission: float = 0


class BotSchema(BaseModel):
    id: str = ""
    pair: str
    balance_size_to_use: float = 0
    balance_to_use: str = "1"
    base_order_size: str = "15"  # Min Binance 0.0001 BNB
    candlestick_interval: str = "15m"
    close_condition: str = ""
    cooldown: int = 0  # cooldown period in minutes before opening next bot with same pair
    created_at: float = time() * 1000
    deal: DealSchema = Field(default_factory=DealSchema)
    dynamic_trailling: bool = False
    errors: list[str] = [] # Event logs
    locked_so_funds: float = 0  # funds locked by Safety orders
    mode: str = "manual"  # Manual is triggered by the terminal dashboard, autotrade by research app
    name: str = "Default bot"
    orders: list[OrderSchema] = []  # Internal
    status: str = "inactive"
    stop_loss: float = 0
    margin_short_reversal: bool = False # If stop_loss > 0, allow for reversal
    take_profit: float = 0
    trailling: bool = True
    trailling_deviation: float = 0
    trailling_profit: float = 0  # Trailling activation (first take profit hit)
    safety_orders: list[SafetyOrderSchema] = []
    strategy: str = "long"
    short_buy_price: float = 0  # > 0 base_order does not execute immediately, executes short strategy when this value is hit
    short_sell_price: float = 0  # autoswitch to short_strategy
    # Deal and orders are internal, should never be updated by outside data
    total_commission: float = 0
    updated_at: float = time() * 1000

    @field_validator("pair", "base_order_size", "candlestick_interval")
    @classmethod
    def check_names_not_empty(cls, v):
        assert v != "", "Empty pair field."
        return v

    @field_validator("stop_loss", "take_profit", "trailling_deviation", "trailling_profit")
    @classmethod
    def check_percentage(cls, v):
        if 0 <= float(v) < 100:
            return v
        else:
            raise ValueError(f"{v} must be a percentage")

    @field_validator("trailling")
    @classmethod
    def check_trailling(cls, v: str | bool):
        if isinstance(v, str) and v.lower() == "false":
            return False
        return True

    @field_validator("errors")
    @classmethod
    def check_errors_format(cls, v: list[str]):
        if not isinstance(v, list):
            raise ValueError(f'Errors must be a list of strings')
        return v

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        json_schema_extra = {
            "description": "Most fields are optional. Deal field is generated internally, orders are filled up by Binance",
            "example": {
                "pair": "BNBUSDT",
                "balance_size_to_use": 0,
                "balance_to_use": 0,
                "base_order_size": 15,
                "candlestick_interval": "15m",
                "cooldown": 0,
                "errors": [],
                "locked_so_funds": 0,
                "mode": "manual",  # Manual is triggered by the terminal dashboard, autotrade by research app,
                "name": "Default bot",
                "orders": [],
                "status": "inactive",
                "stop_loss": 0,
                "take_profit": 2.3,
                "trailling": "true",
                "trailling_deviation": 0.63,
                "trailling_profit": 2.3,
                "safety_orders": [],
                "strategy": "long",
                "short_buy_price": 0,
                "short_sell_price": 0,
                "total_commission": 0,
            },
        }

class ErrorsRequestBody(BaseModel):
    errors: str | list[str]

    @field_validator("errors")
    @classmethod
    def check_names_not_empty(cls, v):
        if isinstance(v, list):
            assert len(v) != 0, "List of errors is empty."
        if isinstance(v, str):
            assert v != "", "Empty pair field."
            return v

        return v

from time import time
from typing import Annotated, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, BeforeValidator, Field, field_validator

from shared.enums import (
    BinanceKlineIntervals,
    CloseConditions,
    DealType,
    OrderSide,
    OrderType,
    Status,
    Strategy,
)


def timestamp():
    return time() * 1000


def ensure_float(value: Any) -> float:
    if isinstance(value, str) or isinstance(value, int):
        return float(value)

    return value


Amount = Annotated[
    float,
    BeforeValidator(ensure_float),
]


class OrderModel(BaseModel):
    order_type: OrderType
    time_in_force: str | None = None
    timestamp: float = 0
    pair: str | None = None
    qty: float
    order_side: OrderSide | None = None
    order_id: int
    price: float | None = None
    status: str | None = None
    deal_type: DealType | None = DealType.base_order

    model_config = {
        "use_enum_values": True,
    }


class DealModel(BaseModel):
    """
    Data model that is used for operations,
    so it should all be numbers (int or float)
    """

    current_price: Amount = Field(default=0)
    take_profit_price: Amount = Field(default=0)
    trailling_stop_loss_price: Amount = Field(
        default=0,
        description="take_profit but for trailling, to avoid confusion, trailling_profit_price always be > trailling_stop_loss_price",
    )
    trailling_profit_price: Amount = Field(default=0)
    stop_loss_price: Amount = Field(default=0)

    # fields for margin trading
    total_interests: float = Field(default=0, gt=-1)
    total_commissions: float = Field(default=0, gt=-1)
    margin_loan_id: int = Field(
        default=0,
        ge=0,
        description="Txid from Binance. This is used to check if there is a loan, 0 means no loan",
    )
    margin_repay_id: int = Field(
        default=0, ge=0, description="= 0, it has not been repaid"
    )

    # Refactored deal prices that combine both margin and spot
    opening_price: Amount = Field(
        default=0,
        description="replaces previous buy_price or short_sell_price/margin_short_sell_price",
    )
    opening_qty: Amount = Field(
        default=0,
        description="replaces previous buy_total_qty or short_sell_qty/margin_short_sell_qty",
    )
    opening_timestamp: Amount = Field(default=0)
    closing_price: Amount = Field(
        default=0,
        description="replaces previous sell_price or short_sell_price/margin_short_sell_price",
    )
    closing_qty: Amount = Field(
        default=0,
        description="replaces previous sell_qty or short_sell_qty/margin_short_sell_qty",
    )
    closing_timestamp: Amount = Field(
        default=0,
        description="replaces previous buy_timestamp or margin/short_sell timestamps",
    )

    @field_validator("margin_loan_id", mode="before")
    @classmethod
    def validate_margin_loan_id(cls, value):
        if isinstance(value, float):
            return int(value)
        else:
            return value

    @field_validator("margin_loan_id", mode="after")
    @classmethod
    def cast_float(cls, value):
        if isinstance(value, float):
            return int(value)
        else:
            return value


class MarginOrderSchema(OrderModel):
    margin_buy_borrow_amount: int = 0
    margin_buy_borrow_asset: str = "USDT"
    is_isolated: bool = False


class BotModel(BaseModel):
    """
    The way SQLModel works causes a lot of errors
    if we combine (with inheritance) both Pydantic models
    and SQLModels. they are not compatible. Thus the duplication
    """

    pair: str
    fiat: str = Field(default="USDC")
    base_order_size: float = Field(
        default=15, description="Min Binance 0.0001 BNB approx 15USD"
    )
    candlestick_interval: BinanceKlineIntervals = Field(
        default=BinanceKlineIntervals.fifteen_minutes,
    )
    close_condition: CloseConditions = Field(
        default=CloseConditions.dynamic_trailling,
    )
    cooldown: int = Field(
        default=0,
        description="cooldown period in minutes before opening next bot with same pair",
    )
    created_at: float = Field(default_factory=timestamp)
    updated_at: float = Field(default_factory=timestamp)
    dynamic_trailling: bool = Field(default=False)
    logs: list = Field(default=[])
    mode: str = Field(default="manual")
    name: str = Field(default="Default bot")
    status: Status = Field(default=Status.inactive)
    stop_loss: float = Field(
        default=0, description="If stop_loss > 0, allow for reversal"
    )
    margin_short_reversal: bool = Field(default=False)
    take_profit: float = Field(default=0)
    trailling: bool = Field(default=True)
    trailling_deviation: float = Field(
        default=0,
        ge=-1,
        le=101,
        description="Trailling activation (first take profit hit)",
    )
    trailling_profit: float = Field(default=0)
    strategy: Strategy = Field(default=Strategy.long)
    total_commission: float = Field(
        default=0, description="autoswitch to short_strategy"
    )

    # Relationships
    id: UUID | None = Field(default_factory=uuid4)
    deal: DealModel = Field(default_factory=DealModel)
    orders: list[OrderModel] = Field(default=[])

    model_config = {
        "use_enum_values": True,
        "arbitrary_types_allowed": True,
        "json_encoders": {UUID: str},
        "json_schema_extra": {
            "description": "Most fields are optional. Deal field is generated internally, orders are filled up by Exchange",
            "examples": [
                {
                    "pair": "BNBUSDT",
                    "fiat": "USDC",
                    "base_order_size": 15,
                    "candlestick_interval": "15m",
                    "cooldown": 0,
                    "logs": [],
                    # Manual is triggered by the terminal dashboard, autotrade by research app,
                    "mode": "manual",
                    "name": "Default bot",
                    "orders": [],
                    "status": "inactive",
                    "stop_loss": 0,
                    "take_profit": 2.3,
                    "trailling": "true",
                    "trailling_deviation": 0.63,
                    "trailling_profit": 2.3,
                    "strategy": "long",
                    "short_buy_price": 0,
                    "short_sell_price": 0,
                    "total_commission": 0,
                }
            ],
        },
    }

    @field_validator("id")
    def deserialize_id(cls, v):
        if isinstance(v, UUID):
            return str(v)
        return True


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

from time import time
from pybinbot import (
    DealType,
)
from pydantic import BaseModel, field_validator, Field
from pybinbot import BotBase, OrderBase, DealBase
from uuid import UUID, uuid4


def timestamp():
    return time() * 1000


def ensure_float(value: str | int | float) -> float:
    if isinstance(value, str) or isinstance(value, int):
        return float(value)

    return value


class OrderModel(OrderBase):
    deal_type: DealType = DealType.base_order
    model_config = {
        "use_enum_values": True,
    }


class DealModel(DealBase):
    """
    Data model that is used for operations,
    so it should all be numbers (int or float)
    """

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


class BotModel(BotBase):
    """
    The way SQLModel works causes a lot of errors
    if we combine (with inheritance) both Pydantic models
    and SQLModels. they are not compatible. Thus the duplication
    """

    id: UUID = Field(default_factory=uuid4)
    deal: DealModel = Field(default_factory=DealModel)
    orders: list[OrderModel] = Field(default=[])

    model_config = {
        "from_attributes": True,
        "use_enum_values": True,
        "json_schema_extra": {
            "description": "BotModel with id, deal, and orders. Deal and orders fields are generated internally and filled by Exchange",
            "examples": [
                {
                    "id": "550e8400-e29b-41d4-a716-446655440000",
                    "pair": "BNBUSDT",
                    "fiat": "USDC",
                    "quote_asset": "USDC",
                    "fiat_order_size": 15,
                    "candlestick_interval": "15m",
                    "close_condition": "dynamic_trailling",
                    "cooldown": 0,
                    "created_at": 1702999999.0,
                    "updated_at": 1702999999.0,
                    "dynamic_trailling": False,
                    "logs": [],
                    "mode": "manual",
                    "name": "Default bot",
                    "status": "inactive",
                    "stop_loss": 0,
                    "take_profit": 2.3,
                    "trailling": True,
                    "trailling_deviation": 0.63,
                    "trailling_profit": 2.3,
                    "margin_short_reversal": False,
                    "strategy": "long",
                    "deal": {},
                    "orders": [],
                }
            ],
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

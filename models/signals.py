from time import time

from bson import ObjectId
from pydantic import BaseModel, ConfigDict, Field, field_validator

from shared.enums import (
    BinanceKlineIntervals,
    BinanceOrderModel,
    BinbotEnums,
    CloseConditions,
    DealModel,
    EnumDefinitions,
    Status,
    Strategy,
    TrendEnum,
)


class BollinguerSpread(BaseModel):
    """
    Pydantic model for the Bollinguer spread.
    (optional)
    """

    band_1: float
    band_2: float


class SignalsConsumer(BaseModel):
    """
    Pydantic model for the signals consumer.
    """

    type: str = Field(default="signal")
    spread: float | None = 0
    current_price: float | None = 0
    msg: str
    symbol: str
    algo: str
    trend: TrendEnum | None = TrendEnum.neutral
    bb_spreads: dict | None = None

    model_config = ConfigDict(
        extra="allow",
    )

    @field_validator("spread", "current_price")
    @classmethod
    def name_must_contain_space(cls, v):
        if v is None:
            return 0
        elif isinstance(v, str):
            return float(v)
        elif isinstance(v, float):
            return v
        else:
            raise ValueError("must be a float or 0")


class BotPayload(BaseModel):
    id: str | None = None
    pair: str
    fiat: str = "USDC"
    base_order_size: float | int | str = 15  # Min Binance 0.0001 BNB
    candlestick_interval: BinanceKlineIntervals = Field(
        default=BinanceKlineIntervals.fifteen_minutes
    )
    close_condition: CloseConditions = Field(default=CloseConditions.dynamic_trailling)
    # cooldown period in minutes before opening next bot with same pair
    cooldown: int = 0
    deal: DealModel = Field(default_factory=DealModel)
    dynamic_trailling: bool = False
    errors: list[str] = []  # Event logs
    # to deprecate in new db
    locked_so_funds: float | None = 0  # funds locked by Safety orders
    mode: str = "manual"
    name: str = "Default bot"
    orders: list[BinanceOrderModel] = []  # Internal
    status: Status = Field(default=Status.inactive)
    stop_loss: float = 0
    margin_short_reversal: bool = False  # If stop_loss > 0, allow for reversal
    take_profit: float = 0
    trailling: bool = True
    trailling_deviation: float = 0
    trailling_profit: float = 0  # Trailling activation (first take profit hit)
    strategy: Strategy = Field(default=Strategy.long)
    short_buy_price: float = 0
    short_sell_price: float = 0  # autoswitch to short_strategy
    # Deal and orders are internal, should never be updated by outside data
    total_commission: float = 0
    created_at: float = time() * 1000
    updated_at: float = time() * 1000

    model_config = {
        "arbitrary_types_allowed": True,
        "use_enum_values": True,
        "json_encoders": {ObjectId: str},
        "json_schema_extra": {
            "description": "Most fields are optional. Deal field is generated internally, orders are filled up by Exchange",
            "examples": [
                {
                    "pair": "BNBUSDT",
                    "fiat": "USDC",
                    "base_order_size": 15,
                    "candlestick_interval": "15m",
                    "cooldown": 0,
                    "errors": [],
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

    @field_validator("pair", "candlestick_interval")
    @classmethod
    def string_not_empty(cls, v):
        assert v != "", "Empty pair field."
        return v

    @field_validator("candlestick_interval")
    @classmethod
    def check_names_not_empty(cls, v):
        if v not in EnumDefinitions.chart_intervals:
            raise ValueError(f"{v} must be a valid candlestick interval")
        return v

    @field_validator("base_order_size", "base_order_size", mode="before")
    @classmethod
    def countables(cls, v):
        if isinstance(v, str):
            return float(v)
        elif isinstance(v, int):
            return float(v)
        elif isinstance(v, float):
            return v
        else:
            raise ValueError(f"{v} must be a number (float, int or string)")

    @field_validator(
        "stop_loss",
        "take_profit",
        "trailling_deviation",
        "trailling_profit",
        mode="before",
    )
    @classmethod
    def check_percentage(cls, v):
        if 0 <= float(v) < 100:
            return v
        else:
            raise ValueError(f"{v} must be a percentage")

    @field_validator("mode")
    @classmethod
    def check_mode(cls, v: str):
        if v not in BinbotEnums.mode:
            raise ValueError(f'Status must be one of {", ".join(BinbotEnums.mode)}')
        return v

    @field_validator("strategy")
    @classmethod
    def check_strategy(cls, v: str):
        if v not in BinbotEnums.strategy:
            raise ValueError(f'Status must be one of {", ".join(BinbotEnums.strategy)}')
        return v

    @field_validator("trailling")
    @classmethod
    def string_booleans(cls, v: str | bool):
        if isinstance(v, str) and v.lower() == "false":
            return False
        if isinstance(v, str) and v.lower() == "true":
            return True
        return v

    @field_validator("errors")
    @classmethod
    def check_errors_format(cls, v: list[str]):
        if not isinstance(v, list):
            raise ValueError("Errors must be a list of strings")
        return v

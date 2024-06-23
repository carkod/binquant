from pydantic import BaseModel, ConfigDict, InstanceOf, ValidationError, field_validator
from enum import Enum

from shared.enums import Status, Strategy


class TrendEnum(str, Enum):
    up_trend = "uptrend"
    down_trend = "downtrend"
    neutral = None


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
    type: str = "signal"
    spread: float | str | None = 0
    current_price: float | str | None = 0
    msg: str
    symbol: str
    algo: str
    trend: TrendEnum | None = TrendEnum.neutral

    model_config = ConfigDict(
        extra='allow',
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
            raise ValueError('must be a float or None')


class BotPayload(BaseModel):
    pair: str
    balance_size_to_use: float = 0
    balance_to_use: str = "USDT"
    base_order_size: str = "15"  # Min Binance 0.0001 BNB
    close_condition: str = ""
    dynamic_trailling: bool = False
    errors: list[str] = [] # Event logs
    mode: str = "autotrade"  # Manual is triggered by the terminal dashboard, autotrade by research app
    name: str = "Default bot"
    status: Status = Status.inactive
    stop_loss: float = 0
    margin_short_reversal: bool = False # If stop_loss > 0, allow for reversal
    take_profit: float = 0
    trailling: bool = True
    trailling_deviation: float = 0
    trailling_profit: float = 0  # Trailling activation (first take profit hit)
    strategy: Strategy
    cooldown: int = 0  # Cooldown in seconds
    short_buy_price: float = 0  # > 0 base_order does not execute immediately, executes short strategy when this value is hit
    short_sell_price: float = 0  # autoswitch to short_strategy

    @field_validator("pair", "base_order_size")
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

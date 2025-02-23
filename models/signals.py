from pydantic import BaseModel, ConfigDict, Field, field_validator

from shared.enums import (
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

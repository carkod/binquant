from pydantic import BaseModel, ConfigDict, Field, field_validator

from shared.enums import Strategy


class HABollinguerSpread(BaseModel):
    """
    Pydantic model for the Heikin Ashi Bollinguer spread.
    (optional)
    Replaces original HABollinguerSpread
    permanently using Heikin Ashi candles.
    """

    bb_high: float
    bb_mid: float
    bb_low: float


class SignalsConsumer(BaseModel):
    """
    Pydantic model for the signals consumer.
    """

    type: str = Field(default="signal")
    spread: float | None = Field(default=0)
    current_price: float | None = Field(default=0)
    msg: str
    symbol: str
    algo: str
    bot_strategy: Strategy = Field(default=Strategy.long)
    bb_spreads: HABollinguerSpread | None
    autotrade: bool = Field(default=True, description="False is paper_trading")

    model_config = ConfigDict(
        extra="allow",
        use_enum_values=True,
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

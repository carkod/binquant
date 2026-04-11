from time import time
from pydantic import ConfigDict, Field
from pybinbot import SignalsConsumer


class SignalCandidate(SignalsConsumer):
    """
    SignalsConsumer but as a candidate for the ranked signals system
    """

    msg: str = Field(default="", description="Message to send on Telegram")
    direction: str = Field(default="", description="Signal direction: buy/sell")
    score: float = Field(default=0, description="Score for ranking signals")
    atr: float = Field(default=0, description="Average True Range for volatility")
    bb_width: float = Field(
        default=0, description="Bollinger Bands width for volatility"
    )
    volume: float = Field(
        default=0, description="Volume at the time of signal generation"
    )
    timestamp: int = Field(default=int(time() * 1000), description="Current ts in ms")

    model_config = ConfigDict(from_attributes=True, extra="allow", use_enum_values=True)

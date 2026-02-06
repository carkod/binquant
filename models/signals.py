from time import time
from pydantic import ConfigDict, Field
from pybinbot import SignalsConsumer


class SignalCandidate(SignalsConsumer):
    """
    SignalsConsumer but as a candidate for the ranked signals system
    """

    direction: str = Field(default="", description="Signal direction: buy/sell")
    score: float = Field(default=0, description="Score for ranking signals")
    atr: float
    bb_width: float
    volume: float
    timestamp: int = Field(default=int(time() * 1000), description="Current ts in ms")

    model_config = ConfigDict(from_attributes=True, extra="allow", use_enum_values=True)

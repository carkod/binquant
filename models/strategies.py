from pydantic import BaseModel, ConfigDict


class BBExtremeReversionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    should_trigger: bool
    action: str | None = None
    reason: str = ""
    rsi_window: int = 2
    rsi_value: float = 50.0
    band_position: float = 0.5
    bb_width: float = 0.0
    bb_mid: float = 0.0
    distance_from_mid_pct: float = 0.0

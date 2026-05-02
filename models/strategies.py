from pydantic import BaseModel, ConfigDict


class GridSignalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    should_trigger: bool
    action: str | None = None
    reason: str = ""
    anchor_price: float = 0.0
    anchor_high: float = 0.0
    anchor_low: float = 0.0
    anchor_window_candles: int = 0
    trigger_move_pct: float = 0.02
    range_width: float = 0.0
    range_drift: float = 0.0
    bb_width: float = 0.0
    band_position: float = 0.5
    rsi_value: float = 50.0
    trend_slope_proxy: float = 0.0

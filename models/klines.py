from pydantic import BaseModel


class KlineModel(BaseModel):
    symbol: str
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    candle_closed: bool
    interval: str

class KlineProducerPayloadModel(BaseModel):
    partition: int
    kline: KlineModel

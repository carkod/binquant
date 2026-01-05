from pydantic import BaseModel


class KlineProduceModel(BaseModel):
    symbol: str
    open_time: str
    close_time: str
    open_price: str
    close_price: str
    high_price: str
    low_price: str
    volume: float

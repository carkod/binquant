import os
from pydantic import BaseModel, Field, PositiveInt
from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker(f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}')
app = FastStream(broker)


class BinanceKlines(BaseModel):
    open_time: int = Field(..., description="Open time in milliseconds", examples=[1591258320000])
    open: float = Field(..., description="Price received as string decimals", examples=[9640.7])
    high: float = Field(..., description="Price received as string decimals", examples=[9642.4])
    low: float = Field(..., description="Price received as string decimals", examples=[9640.6])
    close: float = Field(..., description="Price received as string decimals", examples=[9642.0])
    volume: float = Field(..., description="Price received as string decimals", examples=[206])
    close_time: int = Field(..., description="Open time in milliseconds, can be also last price if candle is not closed", examples=[1591258320000])

@broker.subscriber("in")
@broker.publisher("out")
async def handle_msg(data: BinanceKlines) -> str:
    return f"BinanceKlines: {data.user} - {data.user_id} registered"

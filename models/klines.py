from pydantic import BaseModel, Field, field_validator
from datetime import datetime
# from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, FloatType

class KlineModel(BaseModel):
    symbol: str
    open_time: datetime
    close_time: datetime
    open: str
    high: str
    low: str
    close: str
    volume: float
    candle_closed: bool
    interval: str


    @field_validator("open", "high", "low", "close", mode='before')
    @classmethod
    def check_prices(cls, v: str | int | float):
        if isinstance(v, int | float):
            return str(v)
        return v




# SparkKlineSchema = StructType(
#     [
#         StructField("symbol", StringType(), False),
#         StructField("open_time", LongType(), True),
#         StructField("open", FloatType(), False),
#         StructField("high", FloatType(), False),
#         StructField("low", FloatType(), False),
#         StructField("close", FloatType(), False),
#         StructField("volume", FloatType(), False),
#         StructField("close_time", LongType(), True),
#         StructField("candle_closed", BooleanType(), False),
#         StructField("interval", StringType(), False),
#     ]
# )

class KlineProduceModel(BaseModel):
    symbol: str
    open_time: str
    close_time: str
    open_price: str
    close_price: str
    high_price: str
    low_price: str
    volume: float

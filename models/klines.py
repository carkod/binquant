from pydantic import BaseModel, Field, field_validator, model_serializer
from datetime import datetime
from confluent_kafka.serialization import StringSerializer

# from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, FloatType


class KlineModel(BaseModel):
    symbol: str
    end_time: int
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    candle_closed: bool
    interval: str

    @field_validator("open", "high", "low", "close", mode="before")
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


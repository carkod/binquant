from pydantic import BaseModel, field_validator
from datetime import datetime
# from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, FloatType

class KlineModel(BaseModel):
    symbol: str
    open_time: int
    open: str
    high: str
    low: str
    close: str
    volume: float
    close_time: int
    candle_closed: bool
    interval: str
    timestamp: datetime

    @field_validator("open_time", "close_time", mode='before')
    @classmethod
    def check_timestamps(cls, v: str | int):
        """
        Storing in the database as string
        facilitates deserialization,
        as we use a variety of data processing tools
        (pandas, spark, mongodb, etc.) each using
        a different default data type for numbers.

        We could use datetime objects
        but this would reduce precision (nanosecs)
        """
        if isinstance(v, str):
            return int(v)
        return v

    @field_validator("open", "high", "low", "close", mode='before')
    @classmethod
    def check_prices(cls, v: str | int | float):
        if isinstance(v, int | float):
            return str(v)
        return v


class KlineMetadata(BaseModel):
    partition: int # not using at the moment, because in every initialization it starts from 0, we do need a field in the metadata though


class TimeSeriesKline(KlineModel):
    """
    Kline schema
    """

    metadata: KlineMetadata


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

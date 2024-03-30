from xmlrpc.client import Boolean
from pydantic import BaseModel
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, FloatType


class KlineModel(BaseModel):
    symbol: str
    open_time: str
    open: str
    high: str
    low: str
    close: str
    volume: float
    close_time: str
    candle_closed: bool
    interval: str
    timestamp: datetime


class KlineMetadata(BaseModel):
    partition: int


class TimeSeriesKline(KlineModel):
    """
    Kline schema
    """

    metadata: KlineMetadata


SparkKlineSchema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("open_time", LongType(), True),
        StructField("open", FloatType(), False),
        StructField("high", FloatType(), False),
        StructField("low", FloatType(), False),
        StructField("close", FloatType(), False),
        StructField("volume", FloatType(), False),
        StructField("close_time", LongType(), True),
        StructField("candle_closed", BooleanType(), False),
        StructField("interval", StringType(), False),
    ]
)

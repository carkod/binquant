import logging
import os
from datetime import UTC, datetime
from typing import Any

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from pymongo.command_cursor import CommandCursor

from models.klines import KlineModel, KlineProduceModel
from shared.apis.binance_api import BinanceApi
from shared.enums import BinanceKlineIntervals

load_dotenv()


class KafkaDB(BinanceApi):
    def __init__(self):
        super().__init__()
        client: MongoClient = MongoClient(
            host=os.getenv("MONGO_HOSTNAME"),
            port=int(os.environ["MONGO_PORT"]),
            authSource="admin",
            username=os.getenv("MONGO_AUTH_USERNAME"),
            password=os.getenv("MONGO_AUTH_PASSWORD"),
        )
        self.db = client[os.environ["MONGO_KAFKA_DATABASE"]]

    def get_partitions(self):
        query = self.db.kline.aggregate(
            [
                {"$unwind": "$metadata.partition"},
                {"$addFields": {"partition": "$metadata.partition"}},
                {"$project": {"_id": 0, "symbol": 1, "partition": 1}},
            ]
        )
        data = list(query)
        partition_obj = {}
        for item in data:
            partition_obj[item["symbol"]] = item["partition"]
        return partition_obj

    def get_klines(self, symbol, limit=200, offset=0):
        query = self.db.kline.find(
            {"symbol": symbol},
            limit=limit,
            skip=offset,
        )
        return list(query)

    def get_raw_klines(
        self,
        symbol,
        interval: BinanceKlineIntervals = BinanceKlineIntervals.fifteen_minutes,
        limit=200,
        offset=0,
    ) -> list[KlineProduceModel]:
        """
        Query specifically for display or analytics,
        returns klines ordered by close_time, from oldest to newest
        Includes gap detection and automatic filling from Binance API

        Returns:
            list: Klines in requested interval
        """
        if interval == BinanceKlineIntervals.fifteen_minutes:
            query = self.db.kline.find(
                {"symbol": symbol},
                {"_id": 0, "candle_closed": 0},
                limit=limit,
                skip=offset,
                sort=[
                    ("close_time", ASCENDING)
                ],  # Changed to ASCENDING for oldest to newest
            )
            data = list(query)
        else:
            bin_size = interval.bin_size()
            unit = interval.unit()
            pipeline = [
                {"$match": {"symbol": symbol}},
                {
                    "$sort": {"close_time": ASCENDING}
                },  # Sort ascending first for proper grouping
                {
                    "$group": {
                        "_id": {
                            "time": {
                                "$dateTrunc": {
                                    "date": "$close_time",
                                    "unit": unit,
                                    "binSize": bin_size,
                                },
                            },
                        },
                        "open": {"$first": "$open"},
                        "close": {"$last": "$close"},
                        "high": {"$max": "$high"},
                        "low": {"$min": "$low"},
                        "close_time": {"$last": "$close_time"},
                        "open_time": {"$first": "$open_time"},
                        "volume": {"$sum": "$volume"},
                    }
                },
                {
                    "$sort": {"close_time": ASCENDING}
                },  # Changed to ASCENDING for oldest to newest
                {"$limit": limit},
                {"$skip": offset},
            ]
            agg_query: CommandCursor[Any] = self.db.kline.aggregate(pipeline)
            data = list(agg_query)

        return data

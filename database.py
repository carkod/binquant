import os
from datetime import datetime

from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

from models.klines import KlineModel, KlineProduceModel
from shared.enums import BinanceKlineIntervals

load_dotenv()


class KafkaDB:
    def __init__(self):
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

    def store_klines(self, kline):
        """
        Append metadata and store kline data in MongoDB
        """
        klines = KlineModel(
            end_time=int(kline["T"]),
            symbol=kline["s"],
            open_time=datetime.fromtimestamp(kline["t"] / 1000),
            close_time=datetime.fromtimestamp(kline["T"] / 1000),
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            candle_closed=kline["x"],
            interval=kline["i"],
        )

        data = klines.model_dump()
        self.db.kline.insert_one(data)
        pass

    def get_klines(self, symbol, limit=200, offset=0):
        query = self.db.kline.find(
            {"symbol": symbol},
            limit=limit,
            skip=offset,
        )
        return list(query)

    def raw_klines(
        self,
        symbol,
        interval: BinanceKlineIntervals = BinanceKlineIntervals.fifteen_minutes,
        limit=200,
        offset=0,
    ) -> list[KlineProduceModel]:
        """
        Query specifically for display or analytics,
        returns klines ordered by close_time, from oldest to newest

        Returns:
            list: 15m Klines
        """
        query: CommandCursor | Cursor
        if interval == BinanceKlineIntervals.five_minutes:
            query = self.db.kline.find(
                {"symbol": symbol},
                {"_id": 0, "candle_closed": 0},
                limit=limit,
                skip=offset,
                sort=[("_id", DESCENDING)],
            )
        else:
            bin_size = interval.bin_size()
            unit = interval.unit()
            pipeline = [
                {"$match": {"symbol": symbol}},
                {"$sort": {"close_time": DESCENDING}},
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
            ]
            query = self.db.kline.aggregate(pipeline)
        data = list(query)
        return data

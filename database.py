import os
import pytz
from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient
from pymongo.collection import Collection
from shared.enums import BinanceKlineIntervals
from models.klines import KlineProduceModel, KlineModel
from datetime import datetime

load_dotenv()


class KafkaDB:

    def __init__(self):
        client = MongoClient(
            host=os.getenv("MONGO_HOSTNAME"),
            port=int(os.getenv("MONGO_PORT")),
            authSource="admin",
            username=os.getenv("MONGO_AUTH_USERNAME"),
            password=os.getenv("MONGO_AUTH_PASSWORD"),
        )
        self.db = client[os.getenv("MONGO_KAFKA_DATABASE")]
        self.setup()

    def setup(self) -> Collection:
        list_of_collections = self.db.list_collection_names()
        # Return a list of collections in 'test_db'
        if "kline" not in list_of_collections:
            self.db.create_collection(
                "kline",
                **{
                    "timeseries": {
                        "timeField": "close_time",
                        "metaField": "symbol",
                        "granularity": "minutes",
                    },
                    "expireAfterSeconds": 604800,  # 7 days, minimize server cost
                }
            )

        return

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
            symbol=kline["s"],
            open_time=datetime.fromtimestamp(int(kline["t"]) / 1000, tz=pytz.utc),
            close_time=datetime.fromtimestamp(int(kline["T"]) / 1000, tz=pytz.utc),
            open=kline["o"],
            high=kline["h"],
            low=kline["l"],
            close=kline["c"],
            volume=kline["v"],
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

    def raw_klines(self, symbol, limit=200, offset=0, interval=BinanceKlineIntervals.one_minute.value) -> list[KlineProduceModel]:
        """
        Query specifically for display or analytics,
        returns klines ordered by close_time, from oldest to newest
        
        Returns:
            list: Klines
        """
        if interval == BinanceKlineIntervals.one_minute:
            query = self.db.kline.find(
                {"symbol": symbol},
                {"_id": 0, "metadata": 0, "timestamp": 0, "symbol": 0, "candle_closed": 0},
                limit=limit,
                skip=offset,
                sort=[("_id", DESCENDING)],
            )
        else:
            bin_size = int(interval[:-1])
            unit = BinanceKlineIntervals.unit(interval)
            query = self.db.kline.aggregate([
                {"$match": { "symbol": symbol }},
                {"$group": {
                    "_id": {
                        "_id": "$_id",
                        "symbol": "$symbol",
                        "close_time": {
                        "$dateTrunc": {
                            "date": "$close_time",
                            "unit": unit,
                            "binSize": bin_size
                        },
                    },
                    "open_time": {
                        "$dateTrunc": {
                            "date": "$open_time",
                            "unit": unit,
                            "binSize": bin_size
                        },
                    },
                    },
                    "high": { "$max": "$high" },
                    "low": { "$min": "$low" },
                    "open": { "$first": "$open" },
                    "close": { "$last": "$close" },
                    "volume": { "$sum": "$volume" },
                }},
                {"$unwind": "$_id"},
                {
                    "$addFields": {
                        "symbol": "$_id.symbol",
                        "close_time": "$_id.close_time",
                        "open_time": "$_id.open_time",
                    }
                },
                {"$set": {"_id" : "$_id._id"}},
                {"$limit": limit},
                {"$skip": offset},
                {"$sort": {"close_time": -1}},
                # {"$project": {"_id": 0, "metadata": 0, "timestamp": 0}},
            ])
        return list(query)

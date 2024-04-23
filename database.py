import os
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from models.klines import KlineMetadata, KlineProduceModel, TimeSeriesKline
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
                        "timeField": "timestamp",
                        "metaField": "metadata",
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
        timestamp = round((kline["t"] / 1000), 0)
        klines = TimeSeriesKline(
            metadata=KlineMetadata(partition=0),
            timestamp=datetime.fromtimestamp(timestamp),
            symbol=kline["s"],
            open_time=int(kline["t"]),
            open=kline["o"],
            high=kline["h"],
            low=kline["l"],
            close=kline["c"],
            volume=kline["v"],
            close_time=int(kline["T"]),
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

    def raw_klines(self, symbol, limit=200, offset=0) -> list[KlineProduceModel]:
        """
        Query specifically for display or analytics,
        returns klines ordered by close_time, from oldest to newest
        
        Returns:
            list: Klines
        """
        query = self.db.kline.find(
            {"symbol": symbol},
            {"_id": 0, "metadata": 0, "timestamp": 0, "symbol": 0, "candle_closed": 0},
            limit=limit,
            skip=offset,
            sort=[("_id", DESCENDING)],
        )
        return list(query)

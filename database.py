import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient
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

    def get_interval_minutes(self, interval: BinanceKlineIntervals) -> int:
        """
        Convert interval to minutes for gap detection
        """
        interval_map = {
            BinanceKlineIntervals.one_minute: 1,
            BinanceKlineIntervals.three_minutes: 3,
            BinanceKlineIntervals.five_minutes: 5,
            BinanceKlineIntervals.fifteen_minutes: 15,
            BinanceKlineIntervals.thirty_minutes: 30,
            BinanceKlineIntervals.one_hour: 60,
            BinanceKlineIntervals.two_hours: 120,
            BinanceKlineIntervals.four_hours: 240,
            BinanceKlineIntervals.six_hours: 360,
            BinanceKlineIntervals.eight_hours: 480,
            BinanceKlineIntervals.twelve_hours: 720,
            BinanceKlineIntervals.one_day: 1440,
            BinanceKlineIntervals.three_days: 4320,
            BinanceKlineIntervals.one_week: 10080,
            BinanceKlineIntervals.one_month: 43200,  # Approximate
        }
        return interval_map.get(interval, 15)  # Default to 15 minutes

    def detect_gaps(self, data: list, interval: BinanceKlineIntervals) -> list[tuple]:
        """
        Detect gaps in kline data based on interval
        Returns list of (start_time, end_time) tuples representing gaps
        """
        if len(data) < 2:
            return []

        gaps = []
        interval_minutes = self.get_interval_minutes(interval)
        expected_delta = timedelta(minutes=interval_minutes)

        # Sort data by close_time to ensure proper order
        sorted_data = sorted(
            data, key=lambda x: self.extract_datetime(x) or datetime.min
        )

        for i in range(len(sorted_data) - 1):
            current_item = sorted_data[i]
            next_item = sorted_data[i + 1]

            current_time = self.extract_datetime(current_item)
            next_time = self.extract_datetime(next_item)

            if current_time and next_time:
                actual_delta = next_time - current_time
                # Allow some tolerance for small timing differences
                if actual_delta > expected_delta * 1.5:
                    gaps.append((current_time, next_time))
                    logging.debug(
                        f"Gap detected: {current_time} to {next_time}, expected: {expected_delta}, actual: {actual_delta}"
                    )

        return gaps

    def extract_datetime(self, item: dict) -> datetime | None:
        """
        Extract datetime from various data formats (raw or aggregated)
        """
        try:
            # Handle aggregated data format
            if "_id" in item and "time" in item["_id"]:
                time_val = item["_id"]["time"]
            # Handle raw data format
            elif "close_time" in item:
                time_val = item["close_time"]
            else:
                return None

            # Convert to datetime if it's a string
            if isinstance(time_val, str):
                # Handle ISO format with or without timezone
                if time_val.endswith("Z"):
                    time_val = time_val.replace("Z", "+00:00")
                return datetime.fromisoformat(time_val)
            elif isinstance(time_val, datetime):
                return time_val
            else:
                return None
        except (ValueError, TypeError, KeyError) as e:
            logging.warning(f"Failed to extract datetime from item: {item}, error: {e}")
            return None

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
        Validates timestamp order before storing
        """
        # Extract timestamps for validation
        open_timestamp = kline["t"]  # open time in milliseconds
        close_timestamp = kline["T"]  # close time in milliseconds

        # Validate that open_time < close_time
        if open_timestamp >= close_timestamp:
            logging.error(
                f"Invalid kline data for {kline['s']}: open_time ({open_timestamp}) >= close_time ({close_timestamp}). Skipping storage."
            )
            return

        # Add unique constraint to prevent duplicates
        existing = self.db.kline.find_one(
            {"symbol": kline["s"], "end_time": int(kline["T"]), "interval": kline["i"]}
        )

        if existing:
            logging.debug(f"Kline already exists for {kline['s']} at {kline['T']}")
            return

        klines = KlineModel(
            end_time=datetime.fromtimestamp(kline["T"] / 1000, tz=UTC),
            symbol=kline["s"],
            open_time=datetime.fromtimestamp(kline["t"] / 1000, tz=UTC),
            close_time=datetime.fromtimestamp(kline["T"] / 1000, tz=UTC),
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
        # First, check if we have stored data for this exact interval
        stored_interval_query = self.db.kline.find(
            {"symbol": symbol, "interval": interval.value},
            {"_id": 0, "candle_closed": 0},
            limit=limit,
            skip=offset,
            sort=[("close_time", DESCENDING)],
        )
        stored_data = list(stored_interval_query)

        # If we have enough data for the exact interval, use it
        if len(stored_data) >= limit:
            data = stored_data
        else:
            # Otherwise, use aggregation for intervals other than what we store
            if interval == BinanceKlineIntervals.five_minutes:
                query = self.db.kline.find(
                    {"symbol": symbol},
                    {"_id": 0, "candle_closed": 0},
                    limit=limit,
                    skip=offset,
                    sort=[("close_time", DESCENDING)],
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
                    {"$sort": {"close_time": DESCENDING}},
                    {"$limit": limit},
                    {"$skip": offset},
                ]
                agg_query: CommandCursor[Any] = self.db.kline.aggregate(pipeline)
                data = list(agg_query)

        return data

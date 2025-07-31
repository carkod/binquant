import logging
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

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

    def fill_gaps_from_binance(
        self, symbol: str, gaps: list[tuple], interval: BinanceKlineIntervals
    ):
        """
        Fill gaps by fetching data from Binance API and storing it
        """
        for start_time, end_time in gaps:
            try:
                logging.info(
                    f"Filling gap for {symbol} from {start_time} to {end_time}"
                )

                # Convert datetime to timestamp in milliseconds
                start_timestamp = int(start_time.timestamp() * 1000)
                end_timestamp = int(end_time.timestamp() * 1000)

                # Fetch missing data from Binance
                raw_klines = self._get_raw_klines(
                    pair=symbol,
                    interval=interval.value,
                )

                if not raw_klines:
                    logging.warning(f"No data received from Binance for {symbol}")
                    continue

                filled_count = 0
                # Filter to only the gap period and store each kline
                for kline_data in raw_klines:
                    # Binance kline format: [open_time, open, high, low, close, volume, close_time, ...]
                    if len(kline_data) < 7:
                        continue

                    kline_timestamp = kline_data[
                        6
                    ]  # Close time from Binance kline format

                    if start_timestamp <= kline_timestamp <= end_timestamp:
                        # Convert Binance kline format to our KlineModel format
                        kline = {
                            "s": symbol,  # symbol
                            "t": kline_data[0],  # open time
                            "T": kline_data[6],  # close time
                            "o": kline_data[1],  # open
                            "h": kline_data[2],  # high
                            "l": kline_data[3],  # low
                            "c": kline_data[4],  # close
                            "v": kline_data[5],  # volume
                            "x": True,  # candle closed (we only get completed candles from historical API)
                            "i": interval.value,  # interval
                        }

                        self.store_klines(kline)
                        filled_count += 1

                logging.info(
                    f"Filled {filled_count} klines for {symbol} gap from {start_time} to {end_time}"
                )

            except Exception as e:
                logging.error(
                    f"Error filling gap for {symbol} from {start_time} to {end_time}: {e}"
                )
                continue

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
        Includes gap detection and automatic filling from Binance API

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
                {"$sort": {"close_time": DESCENDING}},
            ]
            query = self.db.kline.aggregate(pipeline)

        data = list(query)

        # Detect gaps in the data
        gaps = self.detect_gaps(data, interval)

        # If gaps are found, fill them and re-query
        if gaps:
            logging.info(
                f"Found {len(gaps)} gaps in {symbol} data for interval {interval.value}. Filling gaps..."
            )
            self.fill_gaps_from_binance(symbol, gaps, interval)

        return data

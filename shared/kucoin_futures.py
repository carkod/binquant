from datetime import datetime
from pybinbot import KucoinRest, KucoinKlineIntervals
from kucoin_universal_sdk.generate.futures.market import (
    GetKlinesReqBuilder,
)
from shared.config import Config


class KucoinFutures(KucoinRest):
    """
    Basic Kucoin Futures order endpoints using KucoinApi as base.

    To be moved to pybinbot (take binbot.exchange_apis instead)
    """

    def __init__(self, key: str, secret: str, passphrase: str):
        self.config = Config()
        self.DEFAULT_LEVERAGE = (
            1.5  # assuming stop loss 3% by default, conservative risk
        )
        self.DEFAULT_MULTIPLIER = 1  # for USDT-M futures
        super().__init__(
            key=key,
            secret=secret,
            passphrase=passphrase,
        )
        self.setup_futures_api()

    def get_ui_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time=None,
        end_time=None,
    ) -> list[list]:
        """
        Get raw klines/candlestick data from Kucoin Futures.

        Args:
            symbol: Trading pair symbol (e.g., "BTC-USDT")
            interval: Kline interval is a string to keep consistency across exchanges and market type
            limit: Number of klines to retrieve (max 1500, default 500)
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)
        Returns:
            List of klines in format compatible with Binance format:
            [timestamp, open, high, low, close, volume, close_time, ...]
        """
        # Compute time window based on limit and interval
        interval_enum = KucoinKlineIntervals(interval)
        granularity = interval_enum.to_minutes()
        interval_ms = KucoinKlineIntervals.get_interval_ms(interval_enum)
        now_ms = int(datetime.now().timestamp() * 1000)
        # Align end_time to interval boundary
        end_time = now_ms - (now_ms % interval_ms)
        start_time = end_time - (limit * interval_ms)

        builder = (
            GetKlinesReqBuilder()
            .set_symbol(symbol)
            .set_granularity(granularity)
            .set_from_(start_time)
            .set_to(end_time)
        )

        request = builder.build()
        response = self.futures_market_api.get_klines(request)

        # Convert Kucoin format to Binance-compatible format
        klines = []
        for kline in response.data:
            open_time = kline[0] * 1000  # convert to ms
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            volume = float(kline[5])
            close_time = open_time + interval_ms - 1

            klines.append(
                [
                    open_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    close_time,
                ]
            )

        return klines

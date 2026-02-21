from time import time
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
        Get raw klines/candlestick data from KuCoin Futures.

        Returns Binance-compatible format:
        [open_time_ms, open, high, low, close, volume, close_time_ms]
        """

        # --- Interval ---
        interval_enum = KucoinKlineIntervals(interval)
        granularity = interval_enum.to_minutes()  # e.g., 15 for 15min
        interval_ms = granularity * 60 * 1000  # 15*60*1000 = 900_000 ms

        # --- UTC now in ms ---
        now_ms = int(time() * 1000)

        # --- Determine start / end times ---
        if end_time is None:
            # Align end_time to the last fully closed candle
            end_time = now_ms - (now_ms % interval_ms)

        if start_time is None:
            start_time = end_time - (limit * interval_ms)

        builder = (
            GetKlinesReqBuilder()
            .set_symbol(symbol)
            .set_granularity(granularity)
            .set_from_(int(start_time))
            .set_to(int(end_time))
        )
        request = builder.build()
        response = self.futures_market_api.get_klines(request)

        # --- Parse response ---
        klines = []
        for kline in response.data:
            open_time_ms = int(kline[0])
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            volume = float(kline[5])

            close_time_ms = open_time_ms + interval_ms - 1

            klines.append(
                [
                    open_time_ms,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    close_time_ms,
                ]
            )

        return klines

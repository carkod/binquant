import asyncio
import logging
import os

from producers.klines_connector import KlinesConnector
from shared.enums import BinanceKlineIntervals

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def main():
    connector = KlinesConnector(
        interval=BinanceKlineIntervals.five_minutes,
    )
    await connector.start_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        logging.error(f"Fatal error in Binquant Producer: {error}", exc_info=True)
        asyncio.run(asyncio.sleep(1))  # allow logging to flush
        asyncio.run(main())

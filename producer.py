import logging
import asyncio

from consumers.signals_provider import SignalsProvider
from producers.base import BaseProducer
from producers.klines_connector import KlinesConnector

logging.basicConfig(
    filename="./binbot-research.log",
    filemode="a",
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

async def main():
    base_producer = BaseProducer()
    producer = base_producer.start_producer()
    connector = KlinesConnector(producer)
    connector.start_stream()
    trade_signals_connector = SignalsProvider(producer)
    trade_signals_connector.publish()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        logging.error(error)
        asyncio.run(main())
        pass

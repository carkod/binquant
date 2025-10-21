import asyncio
import logging
import os
from producers.klines_connector import KlinesConnector

logging.basicConfig(
    level=os.environ["LOG_LEVEL"],
    filename=None,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def main():
    connector = KlinesConnector()
    await connector.start_stream()
    logging.debug("Stream started. Waiting for messages...")
    await asyncio.gather(*(c.run_forever() for c in connector.clients))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        logging.error(f"Error in Binquant Producer: {error}", exc_info=True)

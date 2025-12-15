import asyncio
import logging
import os

from shared.streaming.websocket_factory import WebsocketClientFactory

logging.basicConfig(
    level=os.environ["LOG_LEVEL"],
    filename=None,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def main():
    connector = await WebsocketClientFactory().create_connector()
    await asyncio.gather(*(c.run_forever() for c in connector))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        logging.error(f"Error in Binquant Producer: {error}", exc_info=True)

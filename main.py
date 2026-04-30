"""
Binquant single-process entrypoint.

Replaces the previous two-process Kafka split (producer.py / consumer.py).
Runs the WS ingest coroutine and the kline-processing coroutine in the same
event loop, connected by an in-memory `asyncio.Queue`.
"""

import asyncio
import logging

from pybinbot import configure_logging
from consumers.klines_provider import KlinesProvider
from shared.streaming.websocket_factory import WebsocketClientFactory

configure_logging(force=True)


async def ingest_loop(factory: WebsocketClientFactory) -> None:
    """Connect WS clients; each client pushes klines onto the producer queue."""
    clients = await factory.create_connector()
    await asyncio.gather(*(c.run_forever() for c in clients))


async def consume_loop(klines_provider: KlinesProvider, queue: asyncio.Queue) -> None:
    """
    Drain the producer queue and hand each kline to the strategies pipeline.
    A failing kline is logged and skipped so a single bad message can't take
    down the whole loop.
    """
    await klines_provider.load_data_on_start()
    while True:
        message = await queue.get()
        try:
            await klines_provider.aggregate_data(message)
        except Exception:
            logging.exception("Error processing kline message")
        finally:
            queue.task_done()


async def main() -> None:
    queue: asyncio.Queue = asyncio.Queue()

    factory = WebsocketClientFactory(queue=queue)
    klines_provider = KlinesProvider()

    await asyncio.gather(
        ingest_loop(factory),
        consume_loop(klines_provider, queue),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in binquant main: {e}", exc_info=True)
        raise

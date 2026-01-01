import asyncio
import logging
import os

from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.generate.spot.spot_public.model_klines_event import (
    KlinesEvent,
)
from kucoin_universal_sdk.model.client_option import ClientOptionBuilder
from kucoin_universal_sdk.model.constants import (
    GLOBAL_API_ENDPOINT,
)
from kucoin_universal_sdk.model.websocket_option import WebSocketClientOptionBuilder

from models.klines import KlineProduceModel
from shared.enums import KafkaTopics, KucoinKlineIntervals
from shared.streaming.async_producer import AsyncProducer

logger = logging.getLogger(__name__)


class AsyncKucoinWebsocketClient:
    """
    Async KuCoin WebSocket client.
    Subscriptions are queued and flushed ONLY after WELCOME frame.
    """

    def __init__(self, producer: AsyncProducer):
        self.producer = producer
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self._ws_started = False
        self._queue_processor_task: asyncio.Task | None = None
        # Default to 15min interval
        self.interval = KucoinKlineIntervals.FIFTEEN_MINUTES
        # Track last emission time per symbol for deduplication (15min window)
        self._last_emission: dict[str, int] = {}  # symbol -> timestamp_ms
        self._emission_cooldown_ms = KucoinKlineIntervals.get_interval_ms(
            self.interval.value
        )
        client_option = (
            ClientOptionBuilder()
            .set_key(os.getenv("KUCOIN_API_KEY", ""))
            .set_secret(os.getenv("KUCOIN_API_SECRET", ""))
            .set_passphrase(os.getenv("KUCOIN_API_PASSPHRASE", ""))
            .set_spot_endpoint(GLOBAL_API_ENDPOINT)
            .set_websocket_client_option(WebSocketClientOptionBuilder().build())
            .build()
        )

        self.client = DefaultClient(client_option)
        ws_service = self.client.ws_service()
        self.spot_ws = ws_service.new_spot_public_ws()

        # Start websocket connection immediately
        logger.info("Starting KuCoin websocket connection…")
        self.spot_ws.start()
        self._ws_started = True
        logger.info("KuCoin websocket started, ready for subscriptions")

    async def subscribe_klines(self, symbol: str, interval: str):
        """Subscribe to klines for a symbol. Connection must already be started."""
        # Add small delay to ensure connection is ready
        await asyncio.sleep(0.1)
        self.spot_ws.klines(
            symbol=symbol,
            type=interval,
            callback=self.on_kline,
        )
        logger.info(f"Subscribed to {symbol}")

    async def _process_message_queue(self) -> None:
        """Process messages from the queue and send to Kafka."""
        logger.info("Queue processor started")
        try:
            while True:
                # Wait for messages from the queue
                kline_data = await self.message_queue.get()

                try:
                    logger.debug(
                        f"Processing queued message for {kline_data['symbol']}"
                    )
                    await self.producer.send(
                        topic=KafkaTopics.klines_store_topic.value,
                        value=kline_data[
                            "json"
                        ],  # Already a JSON string, serializer will encode to bytes
                        key=kline_data["symbol"],
                        timestamp=kline_data["timestamp"],
                    )
                    logger.debug(f"Successfully sent {kline_data['symbol']} to Kafka")
                except Exception as e:
                    logger.error(f"Failed to send message to Kafka: {e}", exc_info=True)
                finally:
                    self.message_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Queue processor cancelled")
            raise
        except Exception as e:
            logger.error(f"Queue processor error: {e}", exc_info=True)
            raise

    async def run_forever(self) -> None:
        """
        Keep the main asyncio task alive.
        The WS thread is already running via spot_ws.start().
        Incoming events will trigger callbacks automatically.
        """
        logger.info("run_forever() started, starting queue processor")

        # Start the queue processor task
        self._queue_processor_task = asyncio.create_task(self._process_message_queue())

        try:
            iteration = 0
            while True:
                await asyncio.sleep(1)  # Keep event loop alive
                iteration += 1
                if iteration % 30 == 0:
                    queue_size = self.message_queue.qsize()
                    logger.info(
                        f"run_forever alive: iteration {iteration}, queue_size={queue_size}"
                    )
        except asyncio.CancelledError:
            logger.warning("run_forever: CancelledError - shutting down")
            if self._queue_processor_task:
                self._queue_processor_task.cancel()
                try:
                    await self._queue_processor_task
                except asyncio.CancelledError:
                    pass
            raise
        except Exception as e:
            logger.error(f"run_forever: Unexpected error: {e}", exc_info=True)
            raise
        finally:
            logger.critical("run_forever() EXITING!")

    def on_kline(self, topic, subject, event: KlinesEvent):
        try:
            if topic.startswith("/market/candles:"):
                self.process_kline_stream(symbol=event.symbol, candles=event.candles)
        except Exception as e:
            logger.error(f"Error processing kline event: {e}", exc_info=True)

    def process_kline_stream(self, symbol: str, candles: list[str]) -> None:
        """
        Universal SDK subscription is synchronous, runs in a separate thread.
        Push messages to an asyncio.Queue for the main event loop to process.

        Deduplicates symbols within a 15-minute window to avoid redundant emissions.

        Skip if no candles, few candles or no volume
        """

        if not candles or len(candles) < 6 or float(candles[5]) == 0:
            return

        logger.debug(f"Received kline for {symbol}: {candles}")

        ts = int(candles[0])
        ts_ms = ts * 1000

        # Skip if symbol was emitted within last 15 minutes
        last_emit = self._last_emission.get(symbol, 0)
        if ts_ms - last_emit < self._emission_cooldown_ms:
            logger.debug(
                f"Skipping {symbol}: emitted {(ts_ms - last_emit) // 1000}s ago (cooldown: {self._emission_cooldown_ms // 1000}s)"
            )
            return

        # Update last emission time
        self._last_emission[symbol] = ts_ms

        kline = KlineProduceModel(
            symbol=symbol,
            open_time=str(ts_ms),
            close_time=str((ts + 60) * 1000),
            open_price=str(candles[1]),
            close_price=str(candles[2]),
            high_price=str(candles[3]),
            low_price=str(candles[4]),
            volume=str(candles[5]),
        )

        try:
            # Put message in queue (thread-safe)
            message_data = {
                "symbol": symbol,
                "json": kline.model_dump_json(),
                "timestamp": ts_ms,
            }
            # Queue.put_nowait is thread-safe
            self.message_queue.put_nowait(message_data)
            logger.debug(
                f"Queued message for {symbol}, queue_size={self.message_queue.qsize()}"
            )
        except asyncio.QueueFull:
            logger.error(f"Queue is full! Dropping message for {symbol}")
        except Exception as e:
            logger.error(f"Failed to queue message for {symbol}: {e}", exc_info=True)

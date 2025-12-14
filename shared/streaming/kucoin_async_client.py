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
from shared.enums import KafkaTopics
from shared.streaming.async_producer import AsyncProducer

logger = logging.getLogger(__name__)


class AsyncKucoinWebsocketClient:
    """
    Async KuCoin WebSocket client.
    Subscriptions are queued and flushed ONLY after WELCOME frame.
    """

    def __init__(self, producer: AsyncProducer):
        self.producer = producer

        self.producer = producer
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

    async def subscribe_klines(self, symbol: str, interval: str):
        logger.info("Starting websocket…")
        self.spot_ws.start()

        logger.info("Subscribing to klines…")
        self.spot_ws.klines(
            symbol=symbol,
            type=interval,
            callback=self.on_kline,
        )

        logger.info("KuCoin TCP connection established")

    async def run_forever(self) -> None:
        """
        Keep the main asyncio task alive.
        The WS thread is already running via spot_ws.start().
        Incoming events will trigger callbacks automatically.
        """
        logger.info("Reconnecting loop started (WS already running)")
        while True:
            await asyncio.sleep(1)

    def on_kline(self, topic, subject, event: KlinesEvent):
        if topic.startswith("/market/candles:"):
            self.process_kline_stream(symbol=event.symbol, candles=event.candles)

    def process_kline_stream(self, symbol: str, candles: list[str]) -> None:
        """
        Universal SDK subscription is synchronous
        so producer can't be async.

        But because the producer we use is async,
        we need to create an asyncio task here to send the message asynchronously.
        """
        if not candles or len(candles) < 6:
            return

        logging.debug(f"Received kline for {symbol}: {candles}")

        ts = int(candles[0])

        kline = KlineProduceModel(
            symbol=symbol,
            open_time=str(ts * 1000),
            close_time=str((ts + 60) * 1000),
            open_price=str(candles[1]),
            close_price=str(candles[2]),
            high_price=str(candles[3]),
            low_price=str(candles[4]),
            volume=str(candles[5]),
        )

        asyncio.create_task(
            self.producer.send(
                topic=KafkaTopics.klines_store_topic.value,
                value=kline.model_dump_json(),
                key=symbol,
                timestamp=ts * 1000,
            )
        )

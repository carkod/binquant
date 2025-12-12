import asyncio
import logging
import os
from collections.abc import Coroutine
from concurrent.futures import Future as ConcurrencyFuture
from typing import Any

from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.generate.spot.spot_public.model_klines_event import (
    KlinesEvent,
)
from kucoin_universal_sdk.model.client_option import ClientOptionBuilder
from kucoin_universal_sdk.model.constants import (
    GLOBAL_API_ENDPOINT,
    GLOBAL_BROKER_API_ENDPOINT,
    GLOBAL_FUTURES_API_ENDPOINT,
)
from kucoin_universal_sdk.model.transport_option import TransportOptionBuilder
from kucoin_universal_sdk.model.websocket_option import WebSocketClientOptionBuilder

from models.klines import KlineProduceModel
from shared.enums import KafkaTopics
from shared.streaming.async_producer import AsyncProducer

logger = logging.getLogger(__name__)


class AsyncKucoinWebsocketClient:
    """
    Async wrapper around KuCoin Universal SDK's WebSocket client.

    Provides an async interface compatible with the existing async Binance
    websocket implementation pattern.
    """

    ACTION_SUBSCRIBE = "subscribe"
    ACTION_UNSUBSCRIBE = "unsubscribe"

    def __init__(
        self,
        producer: AsyncProducer,
    ):
        """
        Initialize async Kucoin WebSocket client using the official SDK.

        Args:
            producer: Optional async producer to send messages to Kafka
        """
        self.api_key = os.getenv("KUCOIN_API_KEY", "")
        self.api_secret = os.getenv("KUCOIN_API_SECRET", "")
        self.api_passphrase = os.getenv("KUCOIN_API_PASSPHRASE", "")
        # Build client options
        transport_option = (
            TransportOptionBuilder()
            .set_interceptors([])
            .set_read_timeout(30)
            .set_keep_alive(True)
            .build()
        )

        websocket_option = (
            WebSocketClientOptionBuilder()
            .with_reconnect(self._reconnect)
            .with_reconnect_attempts(-1)
            .with_reconnect_interval(5)
            .build()
        )

        client_option = (
            ClientOptionBuilder()
            .set_key(self.api_key)
            .set_secret(self.api_secret)
            .set_passphrase(self.api_passphrase)
            .set_spot_endpoint(GLOBAL_API_ENDPOINT)
            .set_futures_endpoint(GLOBAL_FUTURES_API_ENDPOINT)
            .set_broker_endpoint(GLOBAL_BROKER_API_ENDPOINT)
            .set_transport_option(transport_option)
            .set_websocket_client_option(websocket_option)
            .build()
        )

        # Create default client
        self._client = DefaultClient(client_option)
        self.ws_service = self._client.ws_service()
        # Create a SpotPublicWS instance for public market subscriptions
        self.spot_public_ws = self.ws_service.new_spot_public_ws()

        # Track subscriptions
        self.subscriptions: dict[str, str] = {}
        self._started = False
        self.producer = producer
        self._loop: asyncio.AbstractEventLoop | None = None

    async def process_kline_stream(self, payload: KlinesEvent) -> None:
        """
        Process KuCoin candlestick (kline) stream message and send to Kafka.
        """
        symbol = payload.symbol
        candles = payload.candles

        if not candles or len(candles) < 6:
            logger.warning("Malformed KuCoin kline data")
            return

        # Map KuCoin candle fields to your KlineProduceModel
        # candles: [time, open, close, high, low, volume, turnover]
        kline = KlineProduceModel(
            symbol=symbol,
            open_time=str(int(candles[0]) * 1000),  # seconds to ms
            close_time=str(
                (int(candles[0]) + 60) * 1000
            ),  # 1min interval, adjust as needed
            open_price=str(candles[1]),
            close_price=str(candles[2]),
            high_price=str(candles[3]),
            low_price=str(candles[4]),
            volume=str(candles[5]),
        )
        await self.producer.send(
            topic=KafkaTopics.klines_store_topic.value,
            value=kline.model_dump_json(),
            key=symbol,
            timestamp=int(candles[0]) * 1000,
        )

    async def on_close(self, ws=None):
        logger.warning("KuCoin WebSocket closed. Attempting to reconnect...")
        await self._reconnect()

    async def _reconnect(self):
        try:
            await self.run_forever()
            logger.info("KuCoin WebSocket reconnected.")
        except Exception as e:
            logger.error(f"Failed to reconnect KuCoin WebSocket: {e}")

    async def on_open(self, ws=None):
        logger.info("KuCoin WebSocket connection opened.")

    async def start(self):
        """Start the SpotPublicWS connection before subscribing."""
        if not self._started:
            # capture the running loop to schedule callbacks from SDK threads
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = None
            await asyncio.get_event_loop().run_in_executor(
                None, self.spot_public_ws.start
            )
            self._started = True
            await self.on_open()
            logger.info("KuCoin SpotPublicWS started")

    async def stop(self):
        """Stop the WebSocket connection."""
        if self.spot_public_ws:
            await asyncio.get_event_loop().run_in_executor(
                None, self.spot_public_ws.stop
            )
            self._started = False
            await self.on_close()
            logger.info("Async KuCoin WebSocket client stopped")

    def _callback(self, topic: str, subject: str, event: KlinesEvent) -> None:
        """Internal callback that routes messages to the async handler."""
        if subject == "trade.candles.update":
            self._schedule(self.process_kline_stream(event))

    def _schedule(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Schedule a coroutine from any thread, logging exceptions."""
        if self._loop and self._loop.is_running():
            fut: ConcurrencyFuture[Any] = asyncio.run_coroutine_threadsafe(
                coro, self._loop
            )

            def _log_future(f: ConcurrencyFuture[Any]) -> None:
                try:
                    f.result()
                except Exception as exc:
                    logger.error("Scheduled task failed: %s", exc, exc_info=True)

            fut.add_done_callback(_log_future)
        else:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)
            except RuntimeError:
                logger.error("No running event loop to schedule coroutine")

    async def subscribe_klines(self, symbol: str, interval: str):
        """
        Subscribe to klines (candlestick) data.

        Args:
            symbol: Trading pair (e.g., "BTC-USDT")
            interval: Interval string (e.g., "1min", "5min", "1hour")
        """
        # Ensure websocket is started before subscribing
        if not self._started:
            await self.start()

        sub_id = await asyncio.get_event_loop().run_in_executor(
            None, self.spot_public_ws.klines, symbol, interval, self._callback
        )
        self.subscriptions[f"{symbol}:{interval}"] = sub_id
        logger.info(f"Subscribed to klines: {symbol} @ {interval}")

    async def subscribe_ticker(self, symbols: list[str]):
        """
        Subscribe to ticker data for one or more symbols.

        Args:
            symbols: List of trading pairs (e.g., ["BTC-USDT", "ETH-USDT"])
        """
        if not self._started:
            await self.start()

        try:
            sub_id = await asyncio.get_event_loop().run_in_executor(
                None, self.spot_public_ws.ticker, symbols, self._callback
            )
            self.subscriptions[f"ticker:{','.join(symbols)}"] = sub_id
            logger.info(f"Subscribed to ticker: {symbols}")
        except Exception as e:
            logger.error(f"Error subscribing to ticker: {e}")

    async def unsubscribe(self, subscription_key: str, id: int | None = None):
        """
        Unsubscribe from a subscription.

        Args:
            subscription_key: The key used when subscribing
            id: Optional subscription ID (not used)
        """
        if not self.spot_public_ws:
            return

        if subscription_key in self.subscriptions:
            sub_id = self.subscriptions[subscription_key]
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, self.spot_public_ws.unsubscribe, sub_id
                )
                del self.subscriptions[subscription_key]
                logger.info(f"Unsubscribed from: {subscription_key}")
            except Exception as e:
                logger.error(f"Error unsubscribing: {e}")

    async def run_forever(self):
        """
        Keep the connection alive, auto-reconnect on disconnect or error.
        """
        while True:
            try:
                # If not started yet, just idle until subscriptions mark started
                if not self._started:
                    await asyncio.sleep(1)
                else:
                    # Wait for disconnect or error
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"run_forever encountered error: {e}", exc_info=True)
                await asyncio.sleep(5)

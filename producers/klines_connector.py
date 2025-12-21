import asyncio
import json
import logging

from models.klines import KlineProduceModel
from shared.apis.binbot_api import BinbotApi
from shared.enums import BinanceKlineIntervals, KafkaTopics
from shared.streaming.async_producer import AsyncProducer
from shared.streaming.async_socket_client import AsyncSpotWebsocketStreamClient


class KlinesConnector(BinbotApi):
    """
    Klines/Candlestick Streams

    Uses 5m interval data to produce data for analytics
    which eventually create signals.
    It's the interface between Binance websocket streams and Kafka producer.
    """

    MAX_MARKETS_PER_CLIENT = 400

    def __init__(
        self,
        interval: BinanceKlineIntervals = BinanceKlineIntervals.five_minutes,
    ) -> None:
        logging.debug("Started Kafka producer SignalsInbound")
        super().__init__()
        self.interval = interval
        # Async Kafka producer wrapper (AIOKafkaProducer) – start in start_stream
        self.producer = AsyncProducer()
        self.autotrade_settings = self.get_autotrade_settings()
        self.clients: list[AsyncSpotWebsocketStreamClient] = []

    async def connect_client(self, on_message, on_close, on_error):
        """Instantiate and start an async websocket client."""
        client = AsyncSpotWebsocketStreamClient(
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
            reconnect=True,  # enable resilient reconnects
            heartbeat_interval=30,
        )
        await client.start()
        return client

    async def handle_close(self, idx, code):
        logging.info(f"WebSocket closed (client {idx}) code={code}; attempting restart")
        # Reconnect and resubscribe
        self.clients[idx] = await self.connect_client(
            lambda c, raw, _idx=idx: asyncio.create_task(self.on_message(_idx, c, raw)),
            lambda ccode, _idx=idx: asyncio.create_task(
                self.handle_close(_idx, ccode)
            ),  # on_close passes code
            lambda c, err, _idx=idx: self.handle_error(_idx, c, err),
        )
        await self.start_stream_for_client(idx)

    def handle_error(self, idx, socket, error):
        logging.error(f"WebSocket error (client {idx}): {error}")
        # Decide whether to raise; raising propagates to on_error callback and may trigger reconnect attempts.
        # For now just log; if severe errors occur frequently escalate.
        return

    async def on_message(self, idx, ws, raw):
        """Handle an incoming websocket message."""
        try:
            res = json.loads(raw)
        except Exception as e:
            logging.error(
                f"Failed to decode message (client {idx}): {e}; raw length={len(str(raw))}"
            )
            return
        event_type = res.get("e")
        if event_type == "kline":
            await self.process_kline_stream(res)
        else:
            # Reduce noise by keeping as debug
            logging.debug(f"Non-kline event received (client {idx}): {event_type}")

    async def start_stream(self) -> None:
        """
        Kline/Candlestick Streams

        The Kline/Candlestick Stream push updates to the current klines/candlestick every second.
        Stream Name: <symbol>@kline_<interval>
        Check BinanceKlineIntervals Enum for possible values
        Update Speed: 2000ms

        Split symbols into chunks of MAX_MARKETS_PER_CLIENT
        """
        # Initialize async Kafka producer (AIOKafkaProducer)
        await self.producer.start()
        symbols = self.get_symbols()
        # Filter to only USDT markets
        symbols = [s for s in symbols if s.get("quote_asset") == "USDT"]
        symbol_chunks = [
            symbols[i : i + self.MAX_MARKETS_PER_CLIENT]
            for i in range(0, len(symbols), self.MAX_MARKETS_PER_CLIENT)
        ]
        for idx, chunk in enumerate(symbol_chunks):
            markets = [
                f"{symbol['id'].lower()}@kline_{self.interval.value}"
                for symbol in chunk
            ]
            logging.debug(
                f"Preparing subscription (client {idx}) markets={len(markets)}"
            )

            # Create started client
            client = await self.connect_client(
                lambda c, raw, _idx=idx: asyncio.create_task(
                    self.on_message(_idx, c, raw)
                ),
                lambda code, _idx=idx: asyncio.create_task(
                    self.handle_close(_idx, code)
                ),
                lambda c, err, _idx=idx: self.handle_error(_idx, c, err),
            )
            self.clients.append(client)
            await self.start_stream_for_client(idx)

    async def start_stream_for_client(self, idx: int) -> None:
        """Send subscription message for a specific client."""
        symbols = self.get_symbols()
        # Filter to only USDT markets
        symbols = [s for s in symbols if s.get("quote_asset") == "USDT"]
        symbol_chunks = [
            symbols[i : i + self.MAX_MARKETS_PER_CLIENT]
            for i in range(0, len(symbols), self.MAX_MARKETS_PER_CLIENT)
        ]
        chunk = symbol_chunks[idx]
        markets = [
            f"{symbol['id'].lower()}@kline_{self.interval.value}" for symbol in chunk
        ]
        await self.clients[idx].send_message_to_server(
            markets,
            action=self.clients[idx].ACTION_SUBSCRIBE,
            id=1,
        )
        logging.info(f"Subscribed client {idx} to {len(markets)} markets")

    async def process_kline_stream(self, result):
        """Updates market data in DB for research."""
        symbol = result["k"].get("s")
        # closed candle
        if symbol and result["k"].get("x"):
            data = result["k"]
            message = KlineProduceModel(
                symbol=data["s"],
                open_time=str(data["t"]),
                close_time=str(data["T"]),
                open_price=str(data["o"]),
                high_price=str(data["h"]),
                low_price=str(data["l"]),
                close_price=str(data["c"]),
                volume=str(data["v"]),
            )
            await self.producer.send(
                topic=KafkaTopics.klines_store_topic.value,
                value=message.model_dump_json(),
                key=data["s"],
                timestamp=int(data["t"]),
            )
        return

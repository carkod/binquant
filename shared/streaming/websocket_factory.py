"""WebSocket client factory for different exchanges.

Provides a factory pattern implementation to create websocket clients
for different exchanges (Binance, Kucoin) without changing existing code.
"""

import logging

from producers.klines_connector import KlinesConnector
from shared.apis.binbot_api import BinbotApi
from shared.enums import ExchangeId
from shared.streaming.async_producer import AsyncProducer
from shared.streaming.async_socket_client import AsyncSpotWebsocketStreamClient
from shared.streaming.kucoin_async_client import (
    AsyncKucoinWebsocketClient,
)

logger = logging.getLogger(__name__)


class WebsocketClientFactory:
    """
    Factory class for creating websocket clients for different exchanges.
    """

    def __init__(self) -> None:
        self.binbot_api = BinbotApi()
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.exchange = ExchangeId(self.autotrade_settings["exchange_id"])
        self.producer = AsyncProducer()

    async def start_kucoin_streams(self) -> list[AsyncKucoinWebsocketClient]:
        await self.producer.start()
        symbols = self.binbot_api.get_symbols()
        client = AsyncKucoinWebsocketClient(producer=self.producer)
        for s in symbols:
            symbol_name = s["base_asset"] + "-" + s["quote_asset"]
            await client.subscribe_klines(symbol_name, interval="1min")

        return [client]

    async def create_connector(
        self,
    ) -> list[AsyncSpotWebsocketStreamClient] | list[AsyncKucoinWebsocketClient]:
        """
        Create a KlinesConnector instance
        based on exchange
        """
        if self.exchange == ExchangeId.KUCOIN:
            return await self.start_kucoin_streams()
        else:
            connector = KlinesConnector()
            await connector.start_stream()
            logging.debug("Stream started. Waiting for messages...")
            return connector.clients

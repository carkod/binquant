"""WebSocket client factory for different exchanges.

Provides a factory pattern implementation to create websocket clients
for different exchanges (Binance, Kucoin) without changing existing code.
"""

import asyncio
import logging

from pybinbot import ExchangeId

from producers.klines_connector import KlinesConnector
from pybinbot import BinbotApi
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
        self.fiat = self.autotrade_settings["fiat"]
        self.exchange = ExchangeId(self.autotrade_settings["exchange_id"])
        self.producer = AsyncProducer()

    def filter_fiat_symbols(self, symbols: list[dict]) -> list[dict]:
        """
        Filter symbols to only include USDT markets.
        """
        return [s for s in symbols if s.get("quote_asset") == self.fiat]

    async def start_stream(self) -> list[AsyncKucoinWebsocketClient]:
        await self.producer.start()
        symbols = self.filter_fiat_symbols(self.binbot_api.get_symbols())
        total = len(symbols)
        clients: list[AsyncKucoinWebsocketClient] = []
        max_per_client = 300

        # Create multiple clients, each subscribing to a chunk of symbols
        for i in range(0, total, max_per_client):
            chunk = symbols[i : i + max_per_client]
            client = AsyncKucoinWebsocketClient(producer=self.producer)
            # Give the websocket a moment to be ready before subscribing
            await asyncio.sleep(0.5)

            for s in chunk:
                symbol_name = s["base_asset"] + "-" + s["quote_asset"]
                await client.subscribe_klines(symbol_name, interval="15min")

            clients.append(client)

        logger.info("Created %d KuCoin clients for %d symbols", len(clients), total)
        return clients

    async def create_connector(
        self,
    ) -> list[AsyncSpotWebsocketStreamClient] | list[AsyncKucoinWebsocketClient]:
        """
        Create a KlinesConnector instance
        based on exchange
        """
        if self.exchange == ExchangeId.KUCOIN:
            return await self.start_stream()
        else:
            connector = KlinesConnector()
            await connector.start_stream()
            return connector.clients

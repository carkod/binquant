import asyncio
import logging
from pybinbot import (
    ExchangeId,
    configure_logging,
    KucoinKlineIntervals,
    BinanceKlineIntervals,
    BinbotApi,
    AsyncSpotWebsocketStreamClient,
    AsyncKucoinWebsocketClient,
    MarketType,
)
from producers.klines_connector import KlinesConnector
from shared.config import Config


configure_logging()


class WebsocketClientFactory:
    """
    Factory class for creating websocket clients for different exchanges.

    Takes a shared `asyncio.Queue` so all clients (Binance, KuCoin spot,
    KuCoin futures) push klines onto the same in-process queue consumed by
    the strategies pipeline.
    """

    MAX_TOPICS_PER_CONNECTION = 300

    def __init__(self, queue: asyncio.Queue) -> None:
        self.config = Config()
        self.binbot_api = BinbotApi(
            base_url=self.config.backend_domain,
            service_email=self.config.service_email,
            service_password=self.config.service_password,
        )
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.fiat = self.autotrade_settings.fiat
        self.exchange = ExchangeId(self.autotrade_settings.exchange_id)
        self.queue = queue
        self.interval = (
            KucoinKlineIntervals.FIFTEEN_MINUTES
            if self.exchange == ExchangeId.KUCOIN
            else BinanceKlineIntervals.fifteen_minutes
        )

    def filter_fiat_symbols(self, symbols: list[dict]) -> list[dict]:
        """
        Filter symbols to only include USDT markets.
        """
        return [s for s in symbols if s.get("quote_asset") == self.fiat]

    async def start_stream(self) -> list[AsyncKucoinWebsocketClient]:
        """
        Start websocket stream for KuCoin SPOT.
        max_per_client is 400 for websockets, but to be safe we use 300 to avoid hitting limits.
        If there are more than 300 symbols, we will create multiple websocket clients.
        """
        all_symbols = self.binbot_api.get_symbols()
        symbols = self.filter_fiat_symbols(all_symbols)
        total = len(symbols)
        clients: list[AsyncKucoinWebsocketClient] = []
        # Create multiple clients, each subscribing to a chunk of symbols
        for i in range(0, total, self.MAX_TOPICS_PER_CONNECTION):
            chunk = symbols[i : i + self.MAX_TOPICS_PER_CONNECTION]
            client = AsyncKucoinWebsocketClient(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passpharse=self.config.kucoin_passphrase,
                queue=self.queue,
                market_type=MarketType.SPOT,
            )
            # Give the websocket a moment to be ready before subscribing
            await asyncio.sleep(0.5)

            for s in chunk:
                symbol_name = s["base_asset"] + "-" + s["quote_asset"]
                await client.subscribe_klines(symbol_name, interval=self.interval.value)

            clients.append(client)

        return clients

    async def start_future_stream(self) -> list[AsyncKucoinWebsocketClient]:
        """
        Start websocket stream for KuCoin Futures.
        It has different endpoints and needs to be separated from SPOT streaming.
        """
        all_symbols = self.binbot_api.get_symbols()
        symbols = self.filter_fiat_symbols(all_symbols)
        futures_symbols = [s for s in symbols if s["id"].endswith("USDTM")]
        total = len(futures_symbols)
        clients: list[AsyncKucoinWebsocketClient] = []
        chunk_count = (
            (total + self.MAX_TOPICS_PER_CONNECTION - 1)
            // self.MAX_TOPICS_PER_CONNECTION
            if total
            else 0
        )
        logging.info(
            "Preparing KuCoin futures websocket subscriptions: symbols=%s, "
            "clients=%s, max_topics_per_connection=%s",
            total,
            chunk_count,
            self.MAX_TOPICS_PER_CONNECTION,
        )

        # Create multiple clients, each subscribing to a chunk of symbols
        total_subscriptions = 0
        for idx, i in enumerate(range(0, total, self.MAX_TOPICS_PER_CONNECTION)):
            chunk = futures_symbols[i : i + self.MAX_TOPICS_PER_CONNECTION]
            client = AsyncKucoinWebsocketClient(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passpharse=self.config.kucoin_passphrase,
                queue=self.queue,
                market_type=MarketType.FUTURES,
            )
            # Give the websocket a moment to be ready before subscribing
            await asyncio.sleep(0.5)

            logging.info(
                "Subscribing KuCoin futures websocket client %s/%s to %s symbols",
                idx + 1,
                chunk_count,
                len(chunk),
            )
            for s in chunk:
                await client.subscribe_klines(s["id"], interval=self.interval.value)
                total_subscriptions += 1

            clients.append(client)

        logging.info(
            "KuCoin futures websocket subscriptions ready: clients=%s, "
            "subscriptions=%s",
            len(clients),
            total_subscriptions,
        )

        return clients

    async def create_connector(
        self,
    ) -> list[AsyncSpotWebsocketStreamClient] | list[AsyncKucoinWebsocketClient]:
        """
        Create a KlinesConnector instance
        based on exchange
        """
        if self.exchange == ExchangeId.KUCOIN:
            clients = await self.start_future_stream()
            return clients
        else:
            connector = KlinesConnector(queue=self.queue)
            await connector.start_stream()
            return connector.clients

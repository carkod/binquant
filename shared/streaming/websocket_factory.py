import asyncio
from pybinbot import (
    ExchangeId,
    configure_logging,
    KucoinKlineIntervals,
    BinanceKlineIntervals,
    BinbotApi,
    AsyncProducer,
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
    """

    def __init__(self) -> None:
        self.config = Config()
        self.binbot_api = BinbotApi(
            base_url=self.config.backend_domain,
            service_email=self.config.service_email,
            service_password=self.config.service_password,
        )
        self.autotrade_settings = self.binbot_api.get_autotrade_settings()
        self.fiat = self.autotrade_settings["fiat"]
        self.exchange = ExchangeId(self.autotrade_settings["exchange_id"])
        self.producer = AsyncProducer(
            host=self.config.kafka_host,
            port=self.config.kafka_port,
        )
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
        await self.producer.start()
        symbols = self.filter_fiat_symbols(self.binbot_api.get_symbols())
        total = len(symbols)
        clients: list[AsyncKucoinWebsocketClient] = []
        max_per_client = 300

        # Create multiple clients, each subscribing to a chunk of symbols
        for i in range(0, total, max_per_client):
            chunk = symbols[i : i + max_per_client]
            client = AsyncKucoinWebsocketClient(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passpharse=self.config.kucoin_passphrase,
                producer=self.producer,
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
        await self.producer.start()
        symbols = self.filter_fiat_symbols(self.binbot_api.get_symbols())
        futures_symbols = [s for s in symbols if s["id"].endswith("USDTM")]
        total = len(futures_symbols)
        clients: list[AsyncKucoinWebsocketClient] = []
        max_per_client = 300

        # Create multiple clients, each subscribing to a chunk of symbols
        for i in range(0, total, max_per_client):
            chunk = futures_symbols[i : i + max_per_client]
            client = AsyncKucoinWebsocketClient(
                key=self.config.kucoin_key,
                secret=self.config.kucoin_secret,
                passpharse=self.config.kucoin_passphrase,
                producer=self.producer,
                market_type=MarketType.FUTURES,
            )
            # Give the websocket a moment to be ready before subscribing
            await asyncio.sleep(0.5)

        for s in chunk:
            await client.subscribe_klines(s["id"], interval=self.interval.value)

        clients.append(client)

        return clients

    async def create_connector(
        self,
    ) -> list[AsyncSpotWebsocketStreamClient] | list[AsyncKucoinWebsocketClient]:
        """
        Create a KlinesConnector instance
        based on exchange
        """
        if self.exchange == ExchangeId.KUCOIN:
            # clients = await self.start_stream()
            clients = await self.start_future_stream()
            return clients
        else:
            connector = KlinesConnector()
            await connector.start_stream()
            return connector.clients

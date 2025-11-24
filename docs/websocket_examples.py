"""
Example demonstrating Binance and Kucoin websocket implementations working side by side.

This example shows how to use the factory pattern to create websocket clients
for both exchanges and process their data in parallel.
"""

import asyncio
import json

from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory


# Example 1: Creating clients for both exchanges
async def example_parallel_clients():
    """Run Binance and Kucoin websocket clients in parallel"""

    async def on_binance_message(client, message):
        data = json.loads(message)
        if data.get("e") == "kline":
            print(f"[Binance] Received kline for {data['s']}")

    async def on_kucoin_message(client, message):
        data = json.loads(message)
        if data.get("type") == "message":
            print(f"[Kucoin] Received message: {data.get('topic')}")

    # Create Binance client
    binance_client = WebsocketClientFactory.create_async_spot_client(
        Exchange.binance,
        on_message=on_binance_message,
        reconnect=True,
    )

    # Create Kucoin client
    kucoin_client = WebsocketClientFactory.create_async_spot_client(
        Exchange.kucoin,
        on_message=on_kucoin_message,
        reconnect=True,
    )

    # Start both clients
    await binance_client.start()
    await kucoin_client.start()

    # Subscribe to klines on both exchanges
    await binance_client.klines(
        markets=["BTCUSDT"], interval="1m", action=binance_client.ACTION_SUBSCRIBE
    )
    await kucoin_client.klines(
        markets=["BTC-USDT"], interval="1min", action=kucoin_client.ACTION_SUBSCRIBE
    )

    # Run both clients
    await asyncio.gather(binance_client.run_forever(), kucoin_client.run_forever())


# Example 2: Using the same handler for multiple exchanges
async def example_unified_handler():
    """Use a unified message handler for both exchanges"""

    class MultiExchangeHandler:
        def __init__(self):
            self.binance_klines = []
            self.kucoin_klines = []

        async def handle_binance(self, client, message):
            data = json.loads(message)
            if data.get("e") == "kline" and data["k"].get("x"):
                # Process Binance closed kline
                kline = data["k"]
                self.binance_klines.append(
                    {
                        "exchange": "binance",
                        "symbol": kline["s"],
                        "close_price": kline["c"],
                        "volume": kline["v"],
                    }
                )
                print(f"Binance kline stored: {kline['s']}")

        async def handle_kucoin(self, client, message):
            data = json.loads(message)
            if data.get("type") == "message" and "/market/candles" in data.get(
                "topic", ""
            ):
                # Process Kucoin kline
                candle_data = data.get("data")
                if candle_data:
                    self.kucoin_klines.append(
                        {
                            "exchange": "kucoin",
                            "symbol": data["subject"],
                            "close_price": candle_data.get("close"),
                            "volume": candle_data.get("volume"),
                        }
                    )
                    print(f"Kucoin kline stored: {data['subject']}")

    handler = MultiExchangeHandler()

    binance_client = WebsocketClientFactory.create_async_spot_client(
        Exchange.binance,
        on_message=handler.handle_binance,
    )

    kucoin_client = WebsocketClientFactory.create_async_spot_client(
        Exchange.kucoin,
        on_message=handler.handle_kucoin,
    )

    await binance_client.start()
    await kucoin_client.start()

    # Subscribe to same pairs on both exchanges
    await binance_client.klines(markets=["BTCUSDT", "ETHUSDT"], interval="5m")
    await kucoin_client.klines(markets=["BTC-USDT", "ETH-USDT"], interval="5min")

    await asyncio.gather(binance_client.run_forever(), kucoin_client.run_forever())


# Example 3: Dynamic exchange selection
async def example_dynamic_exchange(exchange_name: str, symbols: list[str]):
    """Create a client for a specific exchange dynamically"""

    # Map exchange name to enum
    exchange = Exchange(exchange_name.lower())

    async def on_message(client, message):
        print(f"[{exchange_name.upper()}] Message: {message[:100]}...")

    client = WebsocketClientFactory.create_async_spot_client(
        exchange, on_message=on_message, reconnect=True
    )

    await client.start()

    # Subscribe based on exchange
    if exchange == Exchange.binance:
        # Binance uses lowercase symbols without dash
        formatted_symbols = [s.replace("-", "").lower() for s in symbols]
        await client.klines(markets=formatted_symbols, interval="1m")
    elif exchange == Exchange.kucoin:
        # Kucoin uses uppercase symbols with dash
        formatted_symbols = [s.upper() for s in symbols]
        await client.klines(markets=formatted_symbols, interval="1min")

    await client.run_forever()


if __name__ == "__main__":
    # Run example 1: Parallel clients
    # asyncio.run(example_parallel_clients())

    # Run example 2: Unified handler
    # asyncio.run(example_unified_handler())

    # Run example 3: Dynamic exchange selection
    # asyncio.run(example_dynamic_exchange("binance", ["BTC-USDT", "ETH-USDT"]))
    # asyncio.run(example_dynamic_exchange("kucoin", ["BTC-USDT", "ETH-USDT"]))

    print("Examples ready. Uncomment the example you want to run.")

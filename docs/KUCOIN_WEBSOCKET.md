# Kucoin WebSocket Implementation

This document describes the Kucoin WebSocket API implementation added to Binquant alongside the existing Binance implementation.

## Overview

The Kucoin WebSocket implementation follows a factory pattern, allowing developers to easily switch between Binance and Kucoin exchanges without changing existing code structure.

## Architecture

### Factory Pattern

The `WebsocketClientFactory` class provides a unified interface for creating websocket clients for different exchanges:

```python
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

# Create a Binance client (existing functionality)
binance_client = WebsocketClientFactory.create_async_spot_client(
    Exchange.binance,
    on_message=my_message_handler
)

# Create a Kucoin client (new functionality)
kucoin_client = WebsocketClientFactory.create_async_spot_client(
    Exchange.kucoin,
    on_message=my_message_handler
)
```

## Components

### 1. Exchange Enum
Located in `shared/enums.py`:

```python
class Exchange(str, Enum):
    binance = "binance"
    kucoin = "kucoin"
```

### 2. Kucoin WebSocket Clients

#### Synchronous Client
- `KucoinSocketManager`: Thread-based websocket manager
- `KucoinWebsocketClient`: Main websocket client
- `KucoinSpotWebsocketStreamClient`: Convenience wrapper for spot markets

#### Asynchronous Client
- `AsyncKucoinWebsocketClient`: Async websocket client using aiohttp
- `AsyncKucoinSpotWebsocketStreamClient`: Async spot market wrapper

### 3. Kucoin API
Located in `shared/apis/kucoin_api.py`:

```python
from shared.apis.kucoin_api import KucoinApi

api = KucoinApi()
# Get websocket token (required by Kucoin)
token_data = api.get_ws_token()
# Get symbols
symbols = api.get_symbols()
# Get candlestick data
klines = api.get_klines(symbol="BTC-USDT", interval="15min")
```

### 4. Factory Methods

The `WebsocketClientFactory` provides four factory methods:

1. **create_client()** - Synchronous websocket client
2. **create_async_client()** - Asynchronous websocket client
3. **create_spot_client()** - Synchronous spot market client
4. **create_async_spot_client()** - Asynchronous spot market client

## Key Differences: Binance vs Kucoin

| Feature | Binance | Kucoin |
|---------|---------|--------|
| **Subscription Format** | Stream-based (`btcusdt@kline_1m`) | Topic-based (`/market/candles:BTC-USDT_1min`) |
| **Subscribe Action** | `"SUBSCRIBE"` | `"subscribe"` |
| **Unsubscribe Action** | `"UNSUBSCRIBE"` | `"unsubscribe"` |
| **Message Format** | `{"method": "SUBSCRIBE", "params": [...]}` | `{"type": "subscribe", "topic": "...", "id": "1"}` |
| **Ping Format** | WebSocket ping frame | `{"type": "ping", "id": "1"}` |
| **Connection** | Direct WebSocket URL | Requires token from REST API |
| **Symbol Format** | `BTCUSDT` | `BTC-USDT` |

## Usage Examples

### Example 1: Async Kucoin Klines Subscription

```python
import asyncio
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

async def on_message(client, message):
    print(f"Received: {message}")

async def main():
    client = WebsocketClientFactory.create_async_spot_client(
        Exchange.kucoin,
        on_message=on_message,
        reconnect=True
    )

    await client.start()

    # Subscribe to BTC-USDT 1min klines
    await client.klines(
        markets=["BTC-USDT"],
        interval="1min",
        action=client.ACTION_SUBSCRIBE
    )

    # Keep running
    await client.run_forever()

asyncio.run(main())
```

### Example 2: Using Factory to Switch Exchanges

```python
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

def create_klines_client(exchange: Exchange):
    """Create a klines client for any supported exchange"""
    async def on_message(client, message):
        # Handle message based on exchange
        if exchange == Exchange.binance:
            # Process Binance format
            pass
        elif exchange == Exchange.kucoin:
            # Process Kucoin format
            pass

    return WebsocketClientFactory.create_async_spot_client(
        exchange,
        on_message=on_message
    )

# Use with Binance
binance_client = create_klines_client(Exchange.binance)

# Use with Kucoin
kucoin_client = create_klines_client(Exchange.kucoin)
```

### Example 3: Synchronous Client

```python
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

def on_message(client, message):
    print(f"Message: {message}")

# Create synchronous Kucoin client
client = WebsocketClientFactory.create_spot_client(
    Exchange.kucoin,
    on_message=on_message
)

# Subscribe to topic
client.subscribe("/market/ticker:BTC-USDT")
```

## Environment Variables

For Kucoin API authentication (optional, for private endpoints):

```bash
KUCOIN_KEY=your_api_key
KUCOIN_SECRET=your_api_secret
KUCOIN_PASSPHRASE=your_passphrase
```

## Testing

Tests are located in `tests/test_websocket_factory.py`:

```bash
# Run all tests
uv run pytest

# Run only websocket factory tests
uv run pytest tests/test_websocket_factory.py -v
```

## Implementation Notes

1. **Non-Breaking Changes**: All Binance functionality remains unchanged. Kucoin implementation is purely additive.

2. **Factory Pattern**: The factory pattern allows easy extension to support additional exchanges in the future.

3. **Consistent API**: Both Binance and Kucoin clients implement the same interface (subscribe, unsubscribe, ping, stop, etc.)

4. **Reconnection**: Both implementations support automatic reconnection with exponential backoff.

5. **Async Support**: Full async/await support using aiohttp for non-blocking I/O.

## Future Enhancements

Potential areas for future development:

1. Add support for Kucoin private channels (account updates, orders)
2. Implement Kucoin futures websocket streams
3. Add more exchange support (e.g., Coinbase, Kraken)
4. Implement WebSocket connection pooling
5. Add metrics and monitoring

## References

- [Kucoin WebSocket Documentation](https://docs.kucoin.com/#websocket-feed)
- [Kucoin REST API Documentation](https://docs.kucoin.com/#general)
- [Binance WebSocket Documentation](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams)

# Kucoin WebSocket Implementation Using Official SDK

This document describes the Kucoin WebSocket API implementation using the official `kucoin-universal-sdk` package, integrated alongside the existing Binance implementation using a factory pattern.

## Overview

The implementation uses the official KuCoin Universal SDK (v1.3.0+) to provide WebSocket connectivity to KuCoin's trading platform. The SDK is wrapped with a simplified interface that maintains compatibility with the existing Binance websocket pattern used in this project.

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

# Create a Kucoin client (using official SDK)
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

### 2. Kucoin WebSocket SDK Wrapper
Located in `shared/streaming/kucoin_websocket_sdk.py`:

The wrapper provides three main classes:

#### KucoinWebsocketClient (Synchronous - Not Supported)
The official SDK does not provide synchronous API for WebSocket connections.

#### AsyncKucoinWebsocketClient
Async wrapper around the KuCoin Universal SDK's WebSocket client:
- Handles connection lifecycle
- Manages subscriptions
- Provides callbacks for messages, errors, and connection state

#### AsyncKucoinSpotWebsocketStreamClient
Convenience wrapper for spot market streams with additional helper methods.

### 3. Official SDK Integration

The implementation uses the following official SDK components:

```python
from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.model.client_option import ClientOptionBuilder
from kucoin_universal_sdk.model.websocket_option import WebSocketClientOptionBuilder
from kucoin_universal_sdk.generate.spot.spot_public.ws_spot_public import SpotPublicWS
```

### 4. Factory Methods

The `WebsocketClientFactory` provides four factory methods:

1. **create_client()** - Raises NotImplementedError for Kucoin (sync not supported)
2. **create_async_client()** - Asynchronous websocket client
3. **create_spot_client()** - Raises NotImplementedError for Kucoin (sync not supported)
4. **create_async_spot_client()** - Asynchronous spot market client

## Installation

Add `kucoin-universal-sdk` to your dependencies:

```toml
# pyproject.toml
dependencies = [
    ...
    "kucoin-universal-sdk>=1.3.0",
    "websocket-client>=1.5.0",  # Updated constraint for SDK compatibility
    ...
]
```

Install dependencies:
```bash
pip install kucoin-universal-sdk
```

## Configuration

### Environment Variables

For public channels (market data), no authentication is required. For private channels:

```bash
KUCOIN_KEY=your_api_key
KUCOIN_SECRET=your_api_secret
KUCOIN_PASSPHRASE=your_passphrase
```

## Usage Examples

### Example 1: Basic Async Kucoin Klines Subscription

```python
import asyncio
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

async def on_message(client, message):
    """Handle incoming messages from KuCoin"""
    data = message.get("data", {})
    print(f"Received kline data: {data}")

async def main():
    # Create async spot client for KuCoin
    client = WebsocketClientFactory.create_async_spot_client(
        Exchange.kucoin,
        on_message=on_message,
        reconnect=True  # Enable automatic reconnection
    )

    # Start the WebSocket connection
    await client.start()

    # Subscribe to BTC-USDT 1min klines
    await client.subscribe_klines("BTC-USDT", "1min")

    # Keep running
    await client.run_forever()

asyncio.run(main())
```

### Example 2: Multiple Subscriptions

```python
import asyncio
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

async def handle_message(client, message):
    topic = message.get("topic", "")
    if "/market/candles:" in topic:
        print(f"Kline update: {message}")
    elif "/market/ticker:" in topic:
        print(f"Ticker update: {message}")

async def main():
    client = WebsocketClientFactory.create_async_spot_client(
        Exchange.kucoin,
        on_message=handle_message
    )

    await client.start()

    # Subscribe to klines for multiple symbols
    await client.klines(
        markets=["BTC-USDT", "ETH-USDT"],
        interval="5min"
    )

    # Subscribe to ticker data
    await client.subscribe_ticker(["BTC-USDT", "ETH-USDT"])

    await client.run_forever()

asyncio.run(main())
```

### Example 3: Using Factory to Switch Exchanges

```python
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

async def create_client_for_exchange(exchange: Exchange):
    """Create a websocket client for any supported exchange"""
    async def on_message(client, message):
        print(f"[{exchange.value}] Message: {message}")

    client = WebsocketClientFactory.create_async_spot_client(
        exchange,
        on_message=on_message,
        reconnect=True
    )

    await client.start()
    return client

# Use with Binance
binance_client = await create_client_for_exchange(Exchange.binance)

# Use with Kucoin
kucoin_client = await create_client_for_exchange(Exchange.kucoin)
```

## Key Differences: Binance vs Kucoin

| Feature | Binance | Kucoin (SDK) |
|---------|---------|--------------|
| **SDK** | Custom implementation | Official `kucoin-universal-sdk` |
| **Sync Support** | Yes | No (async only) |
| **Authentication** | API key/secret | API key/secret/passphrase |
| **Subscribe Method** | `subscribe("btcusdt@kline_1m")` | `subscribe_klines("BTC-USDT", "1min")` |
| **Symbol Format** | `BTCUSDT` | `BTC-USDT` |
| **Interval Format** | `1m`, `5m`, `1h` | `1min`, `5min`, `1hour` |
| **Message Format** | Binance proprietary | KuCoin SDK event objects |
| **Reconnection** | Custom implementation | SDK-managed |

## Message Format

### Kucoin Message Structure

Messages from the SDK wrapper have the following structure:

```python
{
    "type": "message",
    "topic": "/market/candles:BTC-USDT_1min",
    "subject": "candle",
    "data": {
        # KuCoin SDK event object attributes
        # Example for klines:
        "symbol": "BTC-USDT",
        "candles": [...],
        "time": 1234567890000
    }
}
```

## Testing

Tests are located in `tests/test_websocket_factory.py`:

```bash
# Run all tests
pytest

# Run only websocket factory tests
pytest tests/test_websocket_factory.py -v
```

Test coverage includes:
- Factory pattern for creating clients
- Enum validation
- Client initialization
- SDK integration verification

## Implementation Notes

1. **Official SDK**: Uses the official `kucoin-universal-sdk` package maintained by KuCoin
2. **Non-Breaking**: All Binance functionality remains unchanged
3. **Async Only**: KuCoin SDK only provides async WebSocket API
4. **Auto-Reconnection**: SDK handles reconnection internally with configurable attempts
5. **Public Channels**: No authentication required for market data
6. **Type Safety**: SDK provides strongly-typed event objects

## Advantages of Using Official SDK

1. **Maintained by KuCoin**: Updates and bug fixes from the exchange itself
2. **Complete API Coverage**: Access to all KuCoin WebSocket endpoints
3. **Built-in Features**:
   - Automatic reconnection
   - Subscription management
   - Connection pooling
   - Rate limiting
4. **Type Safety**: Strong typing support for better IDE integration
5. **Future-Proof**: Automatically compatible with new KuCoin features

## Limitations

1. **Synchronous API**: Not available (SDK design decision)
2. **Different Interface**: Cannot be 100% compatible with Binance sync client
3. **Additional Dependency**: Adds `kucoin-universal-sdk` package
4. **Version Constraint**: Requires `websocket-client>=1.5.0` (updated from `==1.5.0`)

## Migration Guide

If you have existing custom KuCoin WebSocket code, migrate to the SDK-based implementation:

### Before (Custom Implementation)
```python
# Not available in this implementation
```

### After (Official SDK)
```python
from shared.enums import Exchange
from shared.streaming.websocket_factory import WebsocketClientFactory

client = WebsocketClientFactory.create_async_spot_client(
    Exchange.kucoin,
    on_message=handle_message
)
await client.start()
await client.subscribe_klines("BTC-USDT", "1min")
```

## Troubleshooting

### SDK Not Found
```bash
pip install kucoin-universal-sdk
```

### WebSocket Client Version Conflict
Update `pyproject.toml`:
```toml
"websocket-client>=1.5.0"  # Instead of ==1.5.0
```

### Authentication Errors
Ensure environment variables are set:
```bash
export KUCOIN_KEY="your_key"
export KUCOIN_SECRET="your_secret"
export KUCOIN_PASSPHRASE="your_passphrase"
```

## References

- [KuCoin Universal SDK GitHub](https://github.com/Kucoin/kucoin-universal-sdk)
- [KuCoin WebSocket Documentation](https://docs.kucoin.com/#websocket-feed)
- [KuCoin REST API Documentation](https://docs.kucoin.com/#general)
- [Binance WebSocket Documentation](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams)

## Future Enhancements

Potential areas for future development:

1. Add support for KuCoin private channels (account updates, orders)
2. Implement KuCoin futures websocket streams
3. Add more exchange support using their official SDKs
4. Implement connection pooling across multiple exchanges
5. Add metrics and monitoring

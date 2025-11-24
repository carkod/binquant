from shared.streaming.kucoin_socket_manager import KucoinWebsocketClient


class KucoinSpotWebsocketStreamClient(KucoinWebsocketClient):
    """Kucoin Spot WebSocket Stream Client

    Convenience wrapper for Kucoin spot market streams.
    Uses topic-based subscriptions with format:
    - Klines: /market/candles:{symbol}_{interval}
    - Ticker: /market/ticker:{symbol}
    """

    ACTION_SUBSCRIBE = "subscribe"
    ACTION_UNSUBSCRIBE = "unsubscribe"

    def __init__(
        self,
        stream_url="wss://ws-api-spot.kucoin.com/",
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
    ):
        super().__init__(
            stream_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_ping=on_ping,
            on_pong=on_pong,
        )

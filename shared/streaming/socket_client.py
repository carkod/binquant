from shared.streaming.socket_manager import BinanceWebsocketClient


class SpotWebsocketStreamClient(BinanceWebsocketClient):
    ACTION_SUBSCRIBE = "SUBSCRIBE"
    ACTION_UNSUBSCRIBE = "UNSUBSCRIBE"

    def __init__(
        self,
        stream_url="wss://stream.binance.com:443",
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        is_combined=False,
    ):
        if is_combined:
            stream_url = stream_url + "/stream"
        else:
            stream_url = stream_url + "/ws"
        super().__init__(
            stream_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_ping=on_ping,
            on_pong=on_pong,
        )

import json
import logging
import threading
import time

from websocket import (
    ABNF,
    WebSocketConnectionClosedException,
    WebSocketException,
    create_connection,
)


class KucoinSocketManager(threading.Thread):
    """Kucoin WebSocket Manager

    Similar to BinanceSocketManager but adapted for Kucoin's WebSocket API.
    Kucoin uses a different connection pattern requiring a token-based endpoint.
    """

    def __init__(
        self,
        stream_url,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        logger=None,
    ):
        threading.Thread.__init__(self)
        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger
        self.stream_url = stream_url
        self.on_message = on_message
        self.on_open = on_open
        self.on_close = on_close
        self.on_ping = on_ping
        self.on_pong = on_pong
        self.on_error = on_error
        self.create_ws_connection()

    def create_ws_connection(self):
        self.logger.debug(
            "Creating connection with Kucoin WebSocket Server: %s", self.stream_url
        )
        self.ws = create_connection(self.stream_url)
        self.logger.debug(
            "Kucoin WebSocket connection has been established: %s", self.stream_url
        )
        self._callback(self.on_open)

    def run(self):
        self.read_data()

    def send_message(self, message):
        self.logger.debug("Sending message to Kucoin WebSocket Server: %s", message)
        self.ws.send(message)

    def ping(self):
        self.ws.ping()

    def read_data(self):
        data: bytes = b""
        while True:
            try:
                op_code, frame = self.ws.recv_data_frame(False)
            except WebSocketException as e:
                if isinstance(e, WebSocketConnectionClosedException):
                    self.logger.error("Lost kucoin websocket connection")
                else:
                    self.logger.error(f"Kucoin websocket exception: {e}")
                raise e
            except Exception as e:
                self.logger.error(f"Exception in read_data: {e}")
                raise e

            if op_code == ABNF.OPCODE_CLOSE:
                self.logger.warning(
                    "CLOSE frame received, closing kucoin websocket connection"
                )
                self._callback(self.on_close)
                break
            elif op_code == ABNF.OPCODE_PING:
                self._callback(self.on_ping, frame.data)
                self.ws.pong("")
                self.logger.debug("Received Ping; PONG frame sent back")
            elif op_code == ABNF.OPCODE_PONG:
                self.logger.debug("Received PONG frame")
                self._callback(self.on_pong)
            else:
                data = frame.data
                if op_code == ABNF.OPCODE_TEXT:
                    frame_data = data.decode("utf-8")
                self._callback(self.on_message, frame_data)

    def close(self):
        if not self.ws.connected:
            self.logger.warn("Kucoin websocket already closed")
        else:
            self.ws.send_close()
        return

    def _callback(self, callback, *args):
        if callback:
            try:
                callback(self, *args)
            except Exception as e:
                self.logger.error(f"Error from callback {callback}: {e}")
                if self.on_error:
                    self.on_error(self, e)


class KucoinWebsocketClient:
    """Kucoin WebSocket Client

    Implements Kucoin's WebSocket protocol which differs from Binance:
    - Uses topic-based subscriptions instead of stream names
    - Requires connection token from REST API
    - Uses different message format
    """

    ACTION_SUBSCRIBE = "subscribe"
    ACTION_UNSUBSCRIBE = "unsubscribe"

    def __init__(
        self,
        stream_url,
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        logger=None,
    ):
        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger
        self.socket_manager = self._initialize_socket(
            stream_url,
            on_message,
            on_open,
            on_close,
            on_error,
            on_ping,
            on_pong,
            logger,
        )

        # start the thread
        self.socket_manager.start()
        self.logger.debug("Kucoin WebSocket Client started.")

    def _initialize_socket(
        self,
        stream_url,
        on_message,
        on_open,
        on_close,
        on_error,
        on_ping,
        on_pong,
        logger,
    ):
        return KucoinSocketManager(
            stream_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_ping=on_ping,
            on_pong=on_pong,
            logger=logger,
        )

    def get_timestamp(self):
        return int(time.time() * 1000)

    def send(self, message: dict):
        self.socket_manager.send_message(json.dumps(message))

    def send_message_to_server(self, message, action=None, id=None):
        if not id:
            id = self.get_timestamp()

        if action != self.ACTION_UNSUBSCRIBE:
            return self.subscribe(message, id=id)
        return self.unsubscribe(message, id=id)

    def subscribe(self, topic, id=None):
        """Subscribe to Kucoin topic(s)

        Kucoin format: {"id": "1", "type": "subscribe", "topic": "/market/ticker:BTC-USDT", "response": true}
        """
        if not id:
            id = self.get_timestamp()
        if self._single_topic(topic):
            topic_list = [topic]
        else:
            topic_list = topic

        for t in topic_list:
            json_msg = json.dumps(
                {"id": str(id), "type": "subscribe", "topic": t, "response": True}
            )
            self.socket_manager.send_message(json_msg)

    def unsubscribe(self, topic: str, id=None):
        """Unsubscribe from Kucoin topic

        Kucoin format: {"id": "1", "type": "unsubscribe", "topic": "/market/ticker:BTC-USDT", "response": true}
        """
        if not id:
            id = self.get_timestamp()

        if not self._single_topic(topic):
            raise ValueError("Invalid topic name, expect a string")

        self.socket_manager.send_message(
            json.dumps(
                {"id": str(id), "type": "unsubscribe", "topic": topic, "response": True}
            )
        )

    def ping(self):
        """Send ping to Kucoin WebSocket Server

        Kucoin uses a different ping format: {"id": "1", "type": "ping"}
        """
        self.logger.debug("Sending ping to Kucoin WebSocket Server")
        id = self.get_timestamp()
        self.socket_manager.send_message(json.dumps({"id": str(id), "type": "ping"}))

    def stop(self, id=None):
        self.socket_manager.close()
        self.socket_manager.join()

    def _single_topic(self, topic):
        if isinstance(topic, str):
            return True
        elif isinstance(topic, list):
            return False
        else:
            raise ValueError("Invalid topic name, expect string or array")

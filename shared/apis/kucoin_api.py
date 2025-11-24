import hashlib
import hmac
import os
import time
from base64 import b64encode
from urllib.parse import urlencode

from dotenv import load_dotenv
from requests import Session, get

from shared.utils import handle_binance_errors

load_dotenv()


class KucoinApi:
    """
    Kucoin Api URLs and utilities

    Similar to BinanceApi but adapted for Kucoin's API structure.
    Kucoin uses different authentication and endpoint patterns.
    """

    BASE = "https://api.kucoin.com"
    WS_PUBLIC_ENDPOINT = f"{BASE}/api/v1/bullet-public"
    WS_PRIVATE_ENDPOINT = f"{BASE}/api/v1/bullet-private"

    secret = os.getenv("KUCOIN_SECRET", "")
    key = os.getenv("KUCOIN_KEY", "")
    passphrase = os.getenv("KUCOIN_PASSPHRASE", "")

    server_time_url = f"{BASE}/api/v1/timestamp"
    account_url = f"{BASE}/api/v1/accounts"
    symbols_url = f"{BASE}/api/v1/symbols"
    ticker_url = f"{BASE}/api/v1/market/allTickers"
    ticker_24hr_url = f"{BASE}/api/v1/market/stats"
    klines_url = f"{BASE}/api/v1/market/candles"
    order_url = f"{BASE}/api/v1/orders"
    order_book_url = f"{BASE}/api/v1/market/orderbook/level2_100"

    def request(self, url, method="GET", session: Session = Session(), **kwargs):
        res = session.request(url=url, method=method, **kwargs)
        # Kucoin returns responses in different format than Binance
        # Using same error handler for now, might need customization
        data = handle_binance_errors(res)
        return data

    def get_server_time(self):
        """Get Kucoin server time in milliseconds"""
        response = get(url=self.server_time_url)
        data = handle_binance_errors(response)
        return data.get("data")

    def _get_signature(
        self, timestamp: str, method: str, endpoint: str, body: str = ""
    ):
        """Generate Kucoin API signature

        Kucoin signature format: timestamp + method + endpoint + body
        """
        str_to_sign = f"{timestamp}{method}{endpoint}{body}"
        signature = b64encode(
            hmac.new(
                self.secret.encode("utf-8"),
                str_to_sign.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        )
        passphrase = b64encode(
            hmac.new(
                self.secret.encode("utf-8"),
                self.passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        )
        return signature.decode(), passphrase.decode()

    def signed_request(self, url, method="GET", payload=None):
        """
        Kucoin signed requests

        Different from Binance - uses headers for authentication instead of query params
        """
        session = Session()
        timestamp = str(int(time.time() * 1000))

        # Parse endpoint from URL
        endpoint = url.replace(self.BASE, "")
        if payload and method == "GET":
            endpoint = f"{endpoint}?{urlencode(payload, True)}"

        body = ""
        if payload and method != "GET":
            import json

            body = json.dumps(payload)

        signature, passphrase = self._get_signature(timestamp, method, endpoint, body)

        session.headers.update(
            {
                "KC-API-KEY": self.key,
                "KC-API-SIGN": signature,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-PASSPHRASE": passphrase,
                "KC-API-KEY-VERSION": "2",
                "Content-Type": "application/json",
            }
        )

        if method == "GET":
            data = self.request(url=url, method=method, params=payload, session=session)
        else:
            data = self.request(url=url, method=method, data=body, session=session)
        return data

    def get_klines(self, symbol, interval="15min", start_at=None, end_at=None):
        """Get Kucoin candlestick data

        Args:
            symbol: Trading pair (e.g., "BTC-USDT")
            interval: Interval (e.g., "1min", "15min", "1hour", "1day")
            start_at: Start time in seconds
            end_at: End time in seconds
        """
        params = {"symbol": symbol, "type": interval}
        if start_at:
            params["startAt"] = start_at
        if end_at:
            params["endAt"] = end_at
        data = self.request(url=self.klines_url, params=params)
        return data

    def get_ticker(self, symbol=None):
        """Get ticker information

        Args:
            symbol: Trading pair (e.g., "BTC-USDT"), if None returns all tickers
        """
        if symbol:
            url = f"{self.BASE}/api/v1/market/orderbook/level1"
            data = self.request(url=url, params={"symbol": symbol})
        else:
            data = self.request(url=self.ticker_url)
        return data

    def get_24hr_stats(self, symbol: str):
        """Get 24hr statistics for a symbol"""
        data = self.request(url=self.ticker_24hr_url, params={"symbol": symbol})
        return data

    def get_symbols(self):
        """Get all trading symbols"""
        data = self.request(url=self.symbols_url)
        return data

    def get_ws_token(self, private=False):
        """Get WebSocket connection token

        Kucoin requires obtaining a token before establishing WebSocket connection

        Args:
            private: Whether to get a private channel token (requires authentication)

        Returns:
            dict with token and ws server info
        """
        if private:
            data = self.signed_request(self.WS_PRIVATE_ENDPOINT, method="POST")
        else:
            data = self.request(url=self.WS_PUBLIC_ENDPOINT, method="POST")
        return data

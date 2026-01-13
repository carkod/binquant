import hashlib
import hmac
import os
from random import randrange
from urllib.parse import urlencode

from dotenv import load_dotenv
from requests import Session, get

from shared.utils import handle_binance_errors

load_dotenv()


class BinanceApi:
    """
    Binance Api URLs
    Picks root url randomly to avoid rate limits
    """

    api_servers = ["https://api.binance.com", "https://api3.binance.com"]
    BASE = api_servers[randrange(3) - 1]
    WS_BASE = "wss://stream.binance.com:9443/stream?streams="

    recvWindow = 5000
    secret = os.getenv("BINANCE_SECRET", "")
    key = os.getenv("BINANCE_KEY", "")
    server_time_url = f"{BASE}/api/v3/time"
    account_url = f"{BASE}/api/v3/account"
    exchangeinfo_url = f"{BASE}/api/v3/exchangeInfo"
    ticker_price_url = f"{BASE}/api/v3/ticker/price"
    ticker24_url = f"{BASE}/api/v3/ticker/24hr"
    candlestick_url = f"{BASE}/api/v3/klines"
    order_url = f"{BASE}/api/v3/order"
    order_book_url = f"{BASE}/api/v3/depth"
    avg_price = f"{BASE}/api/v3/avgPrice"
    open_orders = f"{BASE}/api/v3/openOrders"
    all_orders_url = f"{BASE}/api/v3/allOrders"
    trade_fee = f"{BASE}/sapi/v1/asset/tradeFee"

    user_data_stream = f"{BASE}/api/v3/userDataStream"
    streams_url = f"{WS_BASE}"

    withdraw_url = f"{BASE}/wapi/v3/withdraw.html"
    withdraw_history_url = f"{BASE}/wapi/v3/withdrawHistory.html"
    deposit_history_url = f"{BASE}/wapi/v3/depositHistory.html"
    deposit_address_url = f"{BASE}/wapi/v3/depositAddress.html"

    dust_transfer_url = f"{BASE}/sapi/v1/asset/dust"
    account_snapshot_url = f"{BASE}/sapi/v1/accountSnapshot"
    launchpool_url = (
        "https://launchpad.binance.com/gateway-api/v1/public/launchpool/project/list"
    )
    max_borrow_url = f"{BASE}/sapi/v1/margin/maxBorrowable"

    def request(self, url, method="GET", session: Session = Session(), **kwargs):
        res = session.request(url=url, method=method, **kwargs)
        data = handle_binance_errors(res)
        return data

    def get_server_time(self):
        response = get(url=self.server_time_url)
        data = handle_binance_errors(response)
        return data["serverTime"]

    def signed_request(self, url, method="GET", payload=None):
        """
        USER_DATA, TRADE signed requests
        """
        session = Session()
        query_string = urlencode(payload, True)
        timestamp = self.get_server_time()
        session.headers.update(
            {"Content-Type": "application/json", "X-MBX-APIKEY": self.key}
        )

        if query_string:
            query_string = (
                f"{query_string}&recvWindow={self.recvWindow}&timestamp={timestamp}"
            )
        else:
            query_string = f"recvWindow={self.recvWindow}&timestamp={timestamp}"

        signature = hmac.new(
            self.secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        url = f"{url}?{query_string}&signature={signature}"
        data = self.request(url=url, payload=payload, method=method, session=session)
        return data

    def get_ui_klines(self, symbol, limit=500, interval="15m") -> list:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = self.request(url=self.candlestick_url, params=params)
        return data

    def ticker_price(self, symbol=None):
        """
        Weight 2 (v3). Ideal for list of symbols
        """
        if symbol:
            params = {"symbol": symbol}
        else:
            params = None
        data = self.request(url=self.ticker_price_url, params=params)
        return data

    def ticker_24_price(self, symbol: str) -> float:
        data = self.request(url=self.ticker_price_url, params={"symbol": symbol})
        return float(data["price"])

    def ticker_24(self, symbol: str | None = None):
        """
        Weight 40 without symbol
        https://github.com/carkod/binbot/issues/438

        Using cache
        """
        data = self.request(
            method="GET", url=self.ticker24_url, params={"symbol": symbol}
        )
        return data

    def launchpool_projects(self):
        data = self.request(url=self.launchpool_url, headers={"User-Agent": "Mozilla"})
        return data

    def get_max_borrow(self, asset, symbol: str | None = None):
        return self.signed_request(
            self.max_borrow_url,
            payload={"asset": asset, "isolatedSymbol": symbol},
        )

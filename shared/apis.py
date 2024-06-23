import hashlib
import hmac
import os
from decimal import Decimal
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
    secret = os.getenv("BINANCE_SECRET")
    key = os.getenv("BINANCE_KEY")
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

    def request(self, url, method="GET", session: Session=None, *args,**kwargs):
        if not session:
            session = Session()
        res = session.request(method=method, url=url, *args, **kwargs)
        data = handle_binance_errors(res)
        return data

    def get_server_time(self):
        response = get(url=self.server_time_url)
        data = handle_binance_errors(response)
        return data["serverTime"]

    def signed_request(self, url, method="GET", payload={}):
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
        data = self.request(url, method, session, payload)
        return data

    def _exchange_info(self, symbol=None):
        """
        Copied from /api/account/account.py
        To be refactored in the future
        """
        params = None
        if symbol:
            params = {"symbol": symbol}

        exchange_info = self.request(url=self.exchangeinfo_url, params=params)
        return exchange_info

    def _get_raw_klines(self, pair, limit=500, interval="15m"):
        params = {"symbol": pair, "interval": interval, "limit": limit}
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

    def launchpool_projects(self):
        data = self.request(url=self.launchpool_url, headers={"User-Agent": "Mozilla"})
        return data

    def price_precision(self, symbol):
        """
        Modified from price_filter_by_symbol
        from /api/account/account.py

        This function always will use the tickSize decimals
        """
        symbols = self._exchange_info(symbol)
        market = symbols["symbols"][0]
        price_filter = next(
            (m for m in market["filters"] if m["filterType"] == "PRICE_FILTER"), None
        )

        # Once got the filter data of Binance
        # Transform into string and remove leading zeros
        # This is how the exchange accepts the prices, it will not work with scientific exponential notation e.g. 2.1-10
        price_precision = Decimal(str(price_filter["tickSize"].rstrip(".0")))

        # Finally return the correct number of decimals required
        return -(price_precision).as_tuple().exponent

    def min_amount_check(self, symbol, qty):
        """
        Min amout check
        Uses MIN notional restriction (price x quantity) from /exchangeinfo
        @params:
            - symbol: string - pair/market e.g. BNBBTC
            - Use current ticker price for price
        """
        symbols = self._exchange_info(symbol)
        market = symbols["symbols"][0]
        min_notional_filter = next(
            (m for m in market["filters"] if m["filterType"] == "NOTIONAL"), None
        )
        min_qty = float(qty) > float(min_notional_filter["minNotional"])
        return min_qty

    def find_baseAsset(self, symbol):
        symbols = self._exchange_info(symbol)
        base_asset = symbols["symbols"][0]["baseAsset"]
        return base_asset

    def find_quoteAsset(self, symbol):
        symbols = self._exchange_info(symbol)
        quote_asset = symbols["symbols"][0]
        if quote_asset:
            quote_asset = quote_asset["quoteAsset"]
        return quote_asset



class BinbotApi(BinanceApi):
    """
    API endpoints on this project itself
    includes Binance Api
    """

    bb_base_url = os.getenv("FLASK_DOMAIN")
    bb_24_ticker_url = f"{bb_base_url}/account/ticker24"
    bb_symbols_raw = f"{bb_base_url}/account/symbols"
    bb_bot_url = f"{bb_base_url}/bot"
    bb_activate_bot_url = f"{bb_base_url}/bot/activate"
    bb_gainers_losers = f"{bb_base_url}/account/gainers-losers"
    bb_market_domination = f"{bb_base_url}/account/market-domination"

    # Trade operations
    bb_buy_order_url = f"{bb_base_url}/order/buy"
    bb_tp_buy_order_url = f"{bb_base_url}/order/buy/take-profit"
    bb_buy_market_order_url = f"{bb_base_url}/order/buy/market"
    bb_sell_order_url = f"{bb_base_url}/order/sell"
    bb_tp_sell_order_url = f"{bb_base_url}/order/sell/take-profit"
    bb_sell_market_order_url = f"{bb_base_url}/order/sell/market"
    bb_opened_orders_url = f"{bb_base_url}/order/open"
    bb_close_order_url = f"{bb_base_url}/order/close"
    bb_stop_buy_order_url = f"{bb_base_url}/order/buy/stop-limit"
    bb_stop_sell_order_url = f"{bb_base_url}/order/sell/stop-limit"
    bb_submit_errors = f"{bb_base_url}/bot/errors"
    bb_liquidation_url = f"{bb_base_url}/account/one-click-liquidation"

    # balances
    bb_balance_url = f"{bb_base_url}/account/balance/raw"
    bb_balance_estimate_url = f"{bb_base_url}/account/balance/estimate"
    bb_balance_series_url = f"{bb_base_url}/account/balance/series"
    bb_account_fiat = f"{bb_base_url}/account/fiat"
    bb_available_fiat_url = f"{bb_base_url}/account/fiat/available"

    # research
    bb_autotrade_settings_url = f"{bb_base_url}/autotrade-settings/bots"
    bb_blacklist_url = f"{bb_base_url}/research/blacklist"
    bb_subscribed_list = f"{bb_base_url}/research/subscribed"

    # bots
    bb_active_pairs = f"{bb_base_url}/bot/active-pairs"

    # paper trading
    bb_test_bot_url = f"{bb_base_url}/paper-trading"
    bb_activate_test_bot_url = f"{bb_base_url}/paper-trading/activate"
    bb_test_bot_active_list = f"{bb_base_url}/paper-trading/active-list"
    bb_test_autotrade_url = f"{bb_base_url}/autotrade-settings/paper-trading"

    def get_24_ticker(self, market):
        data = self.request(url=f"{self.bb_24_ticker_url}/{market}")
        return data

    def balance_estimate(self) -> float:
        response = self.request(url=self.bb_balance_estimate_url)
        for balance in response["data"]["balances"]:
            if balance["asset"] == "USDT":
                return float(balance["free"])
        return 0

    def get_available_fiat(self):
        response = self.request(url=self.bb_available_fiat_url)
        return response["data"]

    def get_blacklist(self):
        response = self.request(url=self.bb_blacklist_url)
        return response["data"]

    def update_subscribed_list(self, data):
        response = self.request(url=self.bb_subscribed_list, method="POST", json=data)
        return response["data"]

    def get_market_domination_series(self):
        response = self.request(url=self.bb_market_domination, params={"size": 7})
        return response["data"]

    def blacklist_coin(self, pair, msg):
        response = self.request(self.bb_blacklist_url, method="POST", json={"pair": pair, "reason": msg})
        return response["data"]

    def ticker_24(self, symbol: str | None = None):
        """
        Weight 40 without symbol
        https://github.com/carkod/binbot/issues/438

        Using cache
        """
        data = self.request(method="GET", url=self.ticker24_url, params={"symbol": symbol})
        return data

    def get_latest_btc_price(self):
        # Get 24hr last BTCUSDT
        btc_ticker_24 = self.ticker_24("BTCUSDT")
        self.btc_change_perc = float(btc_ticker_24["priceChangePercent"])
        return self.btc_change_perc

    def post_error(self, msg):
        data = self.request(method="PUT", url=self.bb_autotrade_settings_url, json={"system_logs": msg})
        return data

    def get_test_autotrade_settings(self):
        data = self.request(url=self.bb_test_autotrade_url)
        return data["data"]
    
    def get_autotrade_settings(self):
        data = self.request(url=self.bb_autotrade_settings_url)
        return data["data"]

    def get_bots_by_status(self, status="active", no_cooldown=True, collection_name="bots"):
        url = self.bb_bot_url
        if collection_name == "paper_trading":
            url = self.bb_test_bot_url

        data = self.request(url=url, params={"status": status, "no_cooldown": no_cooldown})
        return data["data"]

    def submit_bot_event_logs(self, bot_id, message):
        data = self.request(url=f"{self.bb_submit_errors}/{bot_id}", method="POST", json={"errors": message})
        return data

    def add_to_blacklist(self, symbol, reason=None):
        payload = {"symbol": symbol, "reason": reason}
        data = self.request(url=self.bb_blacklist_url, method="POST", json=payload)
        return data

    def clean_margin_short(self, pair):
        """
        Liquidate and disable margin_short trades
        """
        data = self.request(url=f"{self.bb_liquidation_url}/{pair}", method="DELETE")
        return data

    def delete_bot(self, bot_id):
        data = self.request(url=f"{self.bb_bot_url}/{bot_id}", method="DELETE")
        return data

    def get_balances(self):
        data = self.request(url=self.bb_balance_url)
        return data

    def create_bot(self, data):
        data = self.request(url=self.bb_bot_url, method="POST", json=data)
        return data

    def activate_bot(self, bot_id):
        data = self.request(url=f"{self.bb_activate_bot_url}/{bot_id}")
        return data

    def create_paper_bot(self, data):
        data = self.request(url=self.bb_test_bot_url, method="POST", json=data)
        return data

    def activate_paper_bot(self, bot_id):
        data = self.request(url=f"{self.bb_activate_test_bot_url}/{bot_id}", method="POST")
        return data

    def get_active_pairs(self):
        """
        Get distinct (non-repeating) bots by status active
        """
        data = self.request(url=f"{self.bb_active_pairs}")
        return data

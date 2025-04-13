import os

from dotenv import load_dotenv

from shared.apis.binance_api import BinanceApi
from shared.enums import Status

load_dotenv()


class BinbotApi(BinanceApi):
    """
    API endpoints on this project itself
    includes Binance Api
    """

    bb_base_url = os.getenv("FLASK_DOMAIN")
    bb_symbols_raw = f"{bb_base_url}/account/symbols"
    bb_bot_url = f"{bb_base_url}/bot"
    bb_activate_bot_url = f"{bb_base_url}/bot/activate"
    bb_gainers_losers = f"{bb_base_url}/account/gainers-losers"
    bb_market_domination = f"{bb_base_url}/charts/market-domination"
    bb_top_gainers = f"{bb_base_url}/charts/top-gainers"
    bb_ticker24 = f"{bb_base_url}/charts/ticker-24"
    bb_btc_correlation_url = f"{bb_base_url}/charts/btc-correlation"

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
    bb_margin_trading_check_url = f"{bb_base_url}/account/check-margin-trading"

    # balances
    bb_balance_url = f"{bb_base_url}/account/balance/raw"
    bb_balance_series_url = f"{bb_base_url}/account/balance/series"
    bb_account_fiat = f"{bb_base_url}/account/fiat"
    bb_available_fiat_url = f"{bb_base_url}/account/fiat/available"

    # research
    bb_autotrade_settings_url = f"{bb_base_url}/autotrade-settings/bots"
    bb_blacklist_url = f"{bb_base_url}/research/blacklist"
    bb_symbols = f"{bb_base_url}/symbols"
    bb_one_symbol_url = f"{bb_base_url}/symbol"

    # bots
    bb_active_pairs = f"{bb_base_url}/bot/active-pairs"

    # paper trading
    bb_test_bot_url = f"{bb_base_url}/paper-trading"
    bb_activate_test_bot_url = f"{bb_base_url}/paper-trading/activate"
    bb_test_bot_active_list = f"{bb_base_url}/paper-trading/active-list"
    bb_test_autotrade_url = f"{bb_base_url}/autotrade-settings/paper-trading"
    bb_test_active_pairs = f"{bb_base_url}/paper/active-pairs"

    def get_available_fiat(self):
        response = self.request(url=self.bb_available_fiat_url)
        return response["data"]

    def get_symbols(self) -> list[dict]:
        response = self.request(url=self.bb_symbols)
        return response["data"]

    def get_single_symbol(self, symbol: str) -> dict:
        response = self.request(url=f"{self.bb_one_symbol_url}/{symbol}")
        return response["data"]

    def get_market_domination_series(self, size=200):
        response = self.request(url=self.bb_market_domination, params={"size": size})
        return response["data"]

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

    def get_latest_btc_price(self):
        # Get 24hr last BTCUSDC
        btc_ticker_24 = self.ticker_24("BTCUSDC")
        self.btc_change_perc = float(btc_ticker_24["priceChangePercent"])
        return self.btc_change_perc

    def post_error(self, msg):
        data = self.request(
            method="PUT", url=self.bb_autotrade_settings_url, json={"system_logs": msg}
        )
        return data

    def get_test_autotrade_settings(self):
        data = self.request(url=self.bb_test_autotrade_url)
        return data["data"]

    def get_autotrade_settings(self):
        data = self.request(url=self.bb_autotrade_settings_url)
        return data["data"]

    def get_bots_by_status(
        self,
        start_date,
        end_date,
        collection_name="bots",
        status=Status.active,
    ):
        url = self.bb_bot_url
        if collection_name == "paper_trading":
            url = self.bb_test_bot_url

        data = self.request(
            url=url,
            params={
                "status": status.value,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        return data["data"]

    def submit_bot_event_logs(self, bot_id, message):
        data = self.request(
            url=f"{self.bb_submit_errors}/{bot_id}",
            method="POST",
            json={"errors": message},
        )
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
        data = self.request(url=self.bb_bot_url, method="POST", data=data)
        return data

    def activate_bot(self, bot_id):
        data = self.request(url=f"{self.bb_activate_bot_url}/{bot_id}")
        return data

    def create_paper_bot(self, data):
        data = self.request(url=self.bb_test_bot_url, method="POST", json=data)
        return data

    def activate_paper_bot(self, bot_id):
        data = self.request(
            url=f"{self.bb_activate_test_bot_url}/{bot_id}", method="POST"
        )
        return data

    def get_active_pairs(self, collection_name="bots"):
        """
        Get distinct (non-repeating) bots by status active
        """
        url = self.bb_active_pairs
        if collection_name == "paper_trading":
            url = self.bb_test_bot_url

        res = self.request(
            url=url,
        )
        return res["data"]

    def margin_trading_check(self, symbol):
        data = self.request(url=f"{self.bb_margin_trading_check_url}/{symbol}")
        return data

    def get_top_gainers(self):
        """
        Top crypto/token/coin gainers of the day
        """
        data = self.request(url=self.bb_top_gainers)
        return data

    def get_btc_correlation(self, symbol) -> float:
        """
        Get BTC correlation
        """
        response = self.request(
            url=self.bb_btc_correlation_url, params={"symbol": symbol}
        )
        data = float(response["data"])
        return data

    def price_precision(self, symbol) -> int:
        """
        Get price decimals from API db
        """
        symbol_info = self.get_single_symbol(symbol)
        return symbol_info["price_precision"]

    def qty_precision(self, symbol) -> int:
        """
        Get qty decimals from API db
        """
        symbol_info = self.get_single_symbol(symbol)
        return symbol_info["qty_precision"]

    def find_base_asset(self, symbol):
        symbol_info = self.get_single_symbol(symbol)
        return symbol_info["base_asset"]

    def find_quote_asset(self, symbol):
        symbol_info = self.get_single_symbol(symbol)
        return symbol_info["quote_asset"]

    def min_amount_check(self, symbol, amount):
        symbol_info = self.get_single_symbol(symbol)
        min_notional = symbol_info["min_notional"]
        return amount > min_notional

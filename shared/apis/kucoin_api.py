import os
import uuid
from datetime import datetime

from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.generate.account.account import (
    GetIsolatedMarginAccountReqBuilder,
    GetSpotAccountListReqBuilder,
)
from kucoin_universal_sdk.generate.account.account.model_get_isolated_margin_account_resp import (
    GetIsolatedMarginAccountResp,
)
from kucoin_universal_sdk.generate.account.transfer.model_flex_transfer_req import (
    FlexTransferReq,
    FlexTransferReqBuilder,
)
from kucoin_universal_sdk.generate.account.transfer.model_flex_transfer_resp import (
    FlexTransferResp,
)
from kucoin_universal_sdk.generate.spot.market import (
    GetAllSymbolsReqBuilder,
    GetKlinesReqBuilder,
    GetPartOrderBookReqBuilder,
    GetSymbolReqBuilder,
)
from kucoin_universal_sdk.model import (
    GLOBAL_API_ENDPOINT,
    ClientOptionBuilder,
    TransportOptionBuilder,
)
from pybinbot import KucoinKlineIntervals


class KucoinApi:
    def __init__(self):
        self.key = os.getenv("KUCOIN_KEY", "")
        self.secret = os.getenv("KUCOIN_SECRET", "")
        self.passphrase = os.getenv("KUCOIN_PASSPHRASE", "")
        self.setup_client()

    def setup_client(self):
        http_transport_option = (
            TransportOptionBuilder()
            .set_keep_alive(True)
            .set_max_pool_size(10)
            .set_max_connection_per_pool(10)
            .build()
        )
        client_option = (
            ClientOptionBuilder()
            .set_key(self.key)
            .set_secret(self.secret)
            .set_passphrase(self.passphrase)
            .set_spot_endpoint(GLOBAL_API_ENDPOINT)
            .set_transport_option(http_transport_option)
            .build()
        )
        self.client = DefaultClient(client_option)
        self.spot_api = self.client.rest_service().get_spot_service().get_market_api()
        self.account_api = (
            self.client.rest_service().get_account_service().get_account_api()
        )
        self.margin_api = (
            self.client.rest_service().get_margin_service().get_market_api()
        )
        self.margin_order_api = (
            self.client.rest_service().get_margin_service().get_order_api()
        )
        self.debit_api = self.client.rest_service().get_margin_service().get_debit_api()
        self.transfer_api = (
            self.client.rest_service().get_account_service().get_transfer_api()
        )
        self.order_api = self.client.rest_service().get_spot_service().get_order_api()

    def get_all_symbols(self):
        request = GetAllSymbolsReqBuilder().build()
        response = self.spot_api.get_all_symbols(request)
        return response

    def get_symbol(self, symbol: str):
        """
        Get single symbol data
        """
        request = GetSymbolReqBuilder().set_symbol(symbol).build()
        response = self.spot_api.get_symbol(request)
        return response

    def get_ticker_price(self, symbol: str) -> float:
        request = GetPartOrderBookReqBuilder().set_symbol(symbol).set_size("1").build()
        response = self.spot_api.get_ticker(request)
        return float(response.price)

    def get_account_balance(self):
        """
        Aggregate all balances from all account types (spot, main, trade, margin, futures).

        The right data shape for Kucion should be provided by
        get_account_balance_by_type method.

        However, this method provides a normalized version for backwards compatibility (Binance) and consistency with current balances table.

        Returns a dict:
            {
                asset:
                    {
                        total: float,
                        breakdown:
                            {
                                    account_type: float, ...
                            }
                    }
            }
        """
        spot_request = GetSpotAccountListReqBuilder().build()
        all_accounts = self.account_api.get_spot_account_list(spot_request)
        balance_items = {}
        for item in all_accounts.data:
            if float(item.balance) > 0:
                balance_items[item.currency] = {
                    "balance": float(item.balance),
                    "free": float(item.available),
                    "locked": float(item.holds),
                }

        margin_request = GetIsolatedMarginAccountReqBuilder().build()
        margin_accounts = self.account_api.get_isolated_margin_account(margin_request)
        if float(margin_accounts.total_asset_of_quote_currency) > 0:
            balance_items["USDT"]["balance"] += float(
                margin_accounts.total_asset_of_quote_currency
            )

        return balance_items

    def get_account_balance_by_type(self):
        """
        Get balances grouped by account type.
        Returns:
            {
                'MAIN': {'USDT': {...}, 'BTC': {...}, ...},
                'TRADE': {'USDT': {...}, ...},
                'MARGIN': {...},
                ...
            }
        Each currency has: balance (total), available, holds
        """
        spot_request = GetSpotAccountListReqBuilder().build()
        all_accounts = self.account_api.get_spot_account_list(spot_request)

        balance_by_type: dict[str, dict[str, dict[str, float]]] = {}
        for item in all_accounts.data:
            if float(item.balance) > 0:
                account_type = item.type  # MAIN, TRADE, MARGIN, etc.
                if account_type not in balance_by_type:
                    balance_by_type[account_type] = {}
                balance_by_type[account_type][item.currency] = {
                    "balance": float(item.balance),
                    "available": float(item.available),
                    "holds": float(item.holds),
                }

        return balance_by_type

    def get_single_spot_balance(self, asset: str) -> float:
        spot_request = GetSpotAccountListReqBuilder().build()
        all_accounts = self.account_api.get_spot_account_list(spot_request)
        total_balance = 0.0
        for item in all_accounts.data:
            if item.currency == asset:
                return float(item.balance)

        return total_balance

    def get_isolated_balance(self, symbol: str) -> GetIsolatedMarginAccountResp:
        request = GetIsolatedMarginAccountReqBuilder().set_symbol(symbol).build()
        response = self.account_api.get_isolated_margin_account(request)
        return response

    def transfer_isolated_margin_to_spot(
        self, asset: str, symbol: str, amount: float
    ) -> FlexTransferResp:
        """
        Transfer funds from isolated margin to spot (main) account.
        `symbol` is the isolated pair like "BTC-USDT".
        """
        client_oid = str(uuid.uuid4())
        req = (
            FlexTransferReqBuilder()
            .set_client_oid(client_oid)
            .set_currency(asset)
            .set_amount(str(amount))
            .set_type(FlexTransferReq.TypeEnum.INTERNAL)
            .set_from_account_type(FlexTransferReq.FromAccountTypeEnum.ISOLATED)
            .set_from_account_tag(symbol)
            .set_to_account_type(FlexTransferReq.ToAccountTypeEnum.MAIN)
            .build()
        )
        return self.transfer_api.flex_transfer(req)

    def transfer_spot_to_isolated_margin(
        self, asset: str, symbol: str, amount: float
    ) -> FlexTransferResp:
        """
        Transfer funds from spot (main) account to isolated margin account.
        `symbol` must be the isolated pair like "BTC-USDT".
        """
        client_oid = str(uuid.uuid4())
        req = (
            FlexTransferReqBuilder()
            .set_client_oid(client_oid)
            .set_currency(asset)
            .set_amount(str(amount))
            .set_type(FlexTransferReq.TypeEnum.INTERNAL)
            .set_from_account_type(FlexTransferReq.FromAccountTypeEnum.MAIN)
            .set_to_account_type(FlexTransferReq.ToAccountTypeEnum.ISOLATED)
            .set_to_account_tag(symbol)
            .build()
        )
        return self.transfer_api.flex_transfer(req)

    def transfer_main_to_trade(self, asset: str, amount: float) -> FlexTransferResp:
        """
        Transfer funds from main to trade (spot) account.
        """
        client_oid = str(uuid.uuid4())
        req = (
            FlexTransferReqBuilder()
            .set_client_oid(client_oid)
            .set_currency(asset)
            .set_amount(str(amount))
            .set_type(FlexTransferReq.TypeEnum.INTERNAL)
            .set_from_account_type(FlexTransferReq.FromAccountTypeEnum.MAIN)
            .set_to_account_type(FlexTransferReq.ToAccountTypeEnum.TRADE)
            .build()
        )
        return self.transfer_api.flex_transfer(req)

    def transfer_trade_to_main(self, asset: str, amount: float) -> FlexTransferResp:
        """
        Transfer funds from trade (spot) account to main.
        """
        client_oid = str(uuid.uuid4())
        req = (
            FlexTransferReqBuilder()
            .set_client_oid(client_oid)
            .set_currency(asset)
            .set_amount(str(amount))
            .set_type(FlexTransferReq.TypeEnum.INTERNAL)
            .set_from_account_type(FlexTransferReq.FromAccountTypeEnum.TRADE)
            .set_to_account_type(FlexTransferReq.ToAccountTypeEnum.MAIN)
            .build()
        )
        return self.transfer_api.flex_transfer(req)

    def get_ui_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
    ) -> list:
        """
        Get raw klines/candlestick data from Kucoin.

        Args:
            symbol: Trading pair symbol (e.g., "BTC-USDT")
            interval: Kline interval (e.g., "15min", "1hour", "1day")
            limit: Number of klines to retrieve (max 1500, default 500)

        Returns:
            List of klines in format compatible with Binance format:
            [timestamp, open, high, low, close, volume, close_time, ...]
        """
        # Compute time window based on limit and interval
        interval_ms = KucoinKlineIntervals.get_interval_ms(interval)
        now_ms = int(datetime.now().timestamp() * 1000)
        # Align end_time to interval boundary
        end_time = now_ms - (now_ms % interval_ms)
        start_time = end_time - (limit * interval_ms)

        builder = (
            GetKlinesReqBuilder()
            .set_symbol(symbol)
            .set_type(interval)
            .set_start_at(int(start_time / 1000))
            .set_end_at(int(end_time / 1000))
        )

        request = builder.build()
        response = self.spot_api.get_klines(request)

        # Derive close_time from open_time + interval, and map turnover to quote_asset_volume
        interval_ms = KucoinKlineIntervals.get_interval_ms(interval)
        raw = response.data or []

        # Build Binance-like klines: [open_time, open, high, low, close, volume, close_time, quote_asset_volume]
        klines: list[list] = []
        # Ensure oldest-first order for downstream rolling/resampling
        for k in reversed(raw):
            # KuCoin payload: [time(sec), open, close, high, low, volume, turnover]
            open_time_ms = int(k[0]) * 1000
            close_time_ms = open_time_ms + interval_ms
            klines.append(
                [
                    open_time_ms,
                    k[1],  # open
                    k[3],  # high
                    k[4],  # low
                    k[2],  # close
                    k[5],  # volume
                    close_time_ms,
                    k[6],  # turnover -> quote_asset_volume
                ]
            )

        return klines

import os
from time import time
import random
import uuid
from kucoin_universal_sdk.api import DefaultClient
from kucoin_universal_sdk.generate.spot.market import (
    GetPartOrderBookReqBuilder,
    GetAllSymbolsReqBuilder,
    GetFullOrderBookReqBuilder,
    GetKlinesReqBuilder,
)
from kucoin_universal_sdk.generate.account.account import (
    GetSpotAccountListReqBuilder,
    GetIsolatedMarginAccountReqBuilder,
)
from kucoin_universal_sdk.model import ClientOptionBuilder
from kucoin_universal_sdk.model import (
    GLOBAL_API_ENDPOINT,
)
from kucoin_universal_sdk.model import TransportOptionBuilder
from kucoin_universal_sdk.generate.account.account.model_get_isolated_margin_account_resp import (
    GetIsolatedMarginAccountResp,
)
from kucoin_universal_sdk.generate.spot.order.model_add_order_sync_resp import (
    AddOrderSyncResp,
)
from kucoin_universal_sdk.generate.spot.order.model_add_order_sync_req import (
    AddOrderSyncReq,
    AddOrderSyncReqBuilder,
)
from kucoin_universal_sdk.generate.spot.order.model_batch_add_orders_sync_req import (
    BatchAddOrdersSyncReqBuilder,
)
from kucoin_universal_sdk.generate.spot.order.model_batch_add_orders_sync_order_list import (
    BatchAddOrdersSyncOrderList,
)
from kucoin_universal_sdk.generate.spot.order.model_cancel_order_by_order_id_sync_req import (
    CancelOrderByOrderIdSyncReqBuilder,
)
from kucoin_universal_sdk.generate.spot.order.model_get_order_by_order_id_req import (
    GetOrderByOrderIdReqBuilder,
)
from kucoin_universal_sdk.generate.spot.order.model_get_open_orders_req import (
    GetOpenOrdersReqBuilder,
)
from kucoin_universal_sdk.generate.margin.order.model_add_order_req import (
    AddOrderReq,
    AddOrderReqBuilder,
)
from kucoin_universal_sdk.generate.margin.order.model_cancel_order_by_order_id_req import (
    CancelOrderByOrderIdReqBuilder,
)
from kucoin_universal_sdk.generate.margin.order.model_get_order_by_order_id_resp import (
    GetOrderByOrderIdResp,
)
from kucoin_universal_sdk.generate.margin.debit.model_repay_req import (
    RepayReqBuilder,
)
from kucoin_universal_sdk.generate.margin.debit.model_repay_resp import (
    RepayResp,
)
from kucoin_universal_sdk.generate.margin.debit.model_borrow_req import (
    BorrowReqBuilder,
)
from kucoin_universal_sdk.generate.margin.debit.model_borrow_resp import (
    BorrowResp,
)
from kucoin_universal_sdk.generate.account.transfer.model_flex_transfer_req import (
    FlexTransferReqBuilder,
    FlexTransferReq,
)
from kucoin_universal_sdk.generate.account.transfer.model_flex_transfer_resp import (
    FlexTransferResp,
)
from shared.enums import KucoinKlineIntervals


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
        balance_items = dict()
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
        start_time=None,
        end_time=None,
    ):
        """
        Get raw klines/candlestick data from Kucoin.

        Args:
            symbol: Trading pair symbol (e.g., "BTC-USDT")
            interval: Kline interval (e.g., "15min", "1hour", "1day")
            limit: Number of klines to retrieve (max 1500, default 500)
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)

        Returns:
            List of klines in format compatible with Binance format:
            [timestamp, open, high, low, close, volume, close_time, ...]
        """
        builder = GetKlinesReqBuilder().set_symbol(symbol).set_type(interval)

        if start_time:
            builder = builder.set_start_at(int(start_time / 1000))
        if end_time:
            builder = builder.set_end_at(int(end_time / 1000))

        request = builder.build()
        response = self.spot_api.get_klines(request)

        interval_ms = KucoinKlineIntervals.get_interval_ms(interval)

        # Convert Kucoin format to Binance-compatible format
        # Kucoin returns: [time, open, close, high, low, volume, turnover]
        # Binance format: [open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, unused_field]
        klines = []
        if response.data:
            for k in response.data[:limit]:
                # k format: [timestamp(seconds), open, close, high, low, volume, turnover]
                open_time = int(k[0]) * 1000  # Convert to milliseconds
                close_time = open_time + interval_ms  # Calculate proper close time
                klines.append(
                    [
                        open_time,  # 0: open_time in milliseconds
                        k[1],  # 1: open
                        k[3],  # 2: high
                        k[4],  # 3: low
                        k[2],  # 4: close
                        k[5],  # 5: volume (base asset)
                        close_time,  # 6: close_time properly calculated
                        k[6],  # 7: quote_asset_volume (turnover)
                        "0",  # 8: number_of_trades (not available in KuCoin)
                        "0",  # 9: taker_buy_base_asset_volume (not available)
                        "0",  # 10: taker_buy_quote_asset_volume (not available)
                        "0",  # 11: unused_field
                    ]
                )

        return klines

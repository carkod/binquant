from shared.apis.binance_api import BinanceApi
from shared.apis.kucoin_api import KucoinApi

CombinedApis = BinanceApi | KucoinApi

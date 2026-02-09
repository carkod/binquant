from shared.config import Config
from pandas import DataFrame
from numpy import isnan, log as logarithm, nan
from pybinbot import (
    BinanceKlineIntervals,
    ExchangeId,
    HABollinguerSpread,
    KucoinKlineIntervals,
    MarketDominance,
    Strategy,
    round_numbers,
    Indicators,
    HeikinAshi,
    BinbotApi,
    KucoinApi,
    BinanceApi,
    AsyncProducer,
)

from algorithms.spike_hunter_v3_kucoin import SpikeHunterV3KuCoin
from algorithms.apex_flow import ApexFlow
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer


class ContextEvaluator:
    def __init__(
        self,
        producer: AsyncProducer,
        api: KucoinApi | BinanceApi,
        symbol: str,
        current_symbol_data,
        top_gainers_day,
        market_breadth_data,
        top_losers_day,
        all_symbols,
        ac_api: AutotradeConsumer,
        exchange: ExchangeId,
        first_seen_at: int,
        interval: BinanceKlineIntervals | KucoinKlineIntervals,
        kucoin_symbol=None,
    ) -> None:
        """
        Only variables no data requests (third party or db)
        or pipeline instances

        Network requested data that doesn't require reloading/real-time/updating
        should be on klines_provider instance
        """
        self.producer = producer
        self.api = api
        self.config = Config()
        self.binbot_api = BinbotApi(base_url=self.config.backend_domain)
        self.symbol = symbol
        self.kucoin_symbol = kucoin_symbol
        self.df = DataFrame()
        self.df_4h = DataFrame()
        self.df_1h = DataFrame()
        self.btc_df = DataFrame()
        self.exchange = exchange
        self.interval = interval
        # describes current USDC market: gainers vs losers
        self.current_market_dominance: MarketDominance = MarketDominance.NEUTRAL
        # describes whether tide is shifting
        self.market_domination_reversal: bool = False
        self.bot_strategy: Strategy = Strategy.long
        self.top_coins_gainers: list[str] = []
        self.top_gainers_day = top_gainers_day
        self.top_losers_day = top_losers_day
        self.market_breadth_data = market_breadth_data
        self.btc_correlation: float = 0
        self.repeated_signals: dict = {}
        self.all_symbols = all_symbols
        # theorically current_symbol_data is always defined
        # if it's not defined, then it wouldn't subscribe with websockets
        self.current_symbol_data = current_symbol_data
        self.price_precision = (
            self.current_symbol_data["price_precision"]
            if self.current_symbol_data
            else 1
        )
        self.telegram_consumer = TelegramConsumer()
        self.at_consumer = ac_api
        # Countdown for Apex Flow score system
        self.first_seen_at = first_seen_at

    def days(self, secs):
        return secs * 86400

    def dynamic_btc_beta_corr(self, window=50) -> tuple[float, float]:
        """
        Rolling beta and correlation of asset returns vs BTC returns
        Caches returns for BTC but not for the asset

        - Correlation = move
        - Beta = magnitude
        """
        if "returns" not in self.df_btc:
            self.df_btc["returns"] = logarithm(
                self.df_btc["close"] / self.df_btc["close"].shift(1)
            )

        self.df["returns"] = logarithm(self.df["close"] / self.df["close"].shift(1))

        # Align returns
        returns = (
            self.df[["returns"]]
            .join(self.df_btc["returns"], how="inner", rsuffix="_btc")
            .dropna()
        )
        returns.columns = ["alt", "btc"]

        if len(returns) < window:
            return 0.0, 0.0

        # Use aligned returns for rolling calculations
        cov = returns["alt"].rolling(window).cov(returns["btc"])
        var = returns["btc"].rolling(window).var()

        beta_series = cov / var.replace(0, nan)

        beta = beta_series.iloc[-1]
        corr = returns["alt"].rolling(window).corr(returns["btc"]).iloc[-1]

        beta_value = round_numbers(beta, 6) if not isnan(beta) else 0.0
        corr_value = 0.0 if isnan(corr) else round_numbers(corr, 6)

        return beta_value, corr_value

    def bb_spreads(self) -> HABollinguerSpread:
        """
        Calculate Heikin Ashi Bollinguer bands spreads for trailling strategies
        """
        bb_high = float(self.df.bb_upper.iloc[-1])
        bb_mid = float(self.df.bb_mid.iloc[-1])
        bb_low = float(self.df.bb_lower.iloc[-1])
        return HABollinguerSpread(
            bb_high=round_numbers(bb_high, 6),
            bb_mid=round_numbers(bb_mid, 6),
            bb_low=round_numbers(bb_low, 6),
        )

    def symbol_dependent_data(self):
        """
        Reload symbol-dependent data such as price and qty precision
        """
        self.current_symbol_data = [
            s for s in self.all_symbols if s["id"] == self.symbol
        ][0]
        self.price_precision = self.current_symbol_data["price_precision"]
        self.qty_precision = self.current_symbol_data["qty_precision"]

    def load_algorithms(self):
        """
        Initialize algorithm instances only once
        they must be loaded after post data processing
        """
        self.sh3 = SpikeHunterV3KuCoin(cls=self)
        self.af = ApexFlow(cls=self)

    async def process_data(self, candles, btc_candles=None):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """
        self.symbol_dependent_data()
        self.df, self.df_1h, self.df_4h = HeikinAshi().pre_process(
            self.exchange, candles
        )
        self.df_btc, _, _ = HeikinAshi().pre_process(self.exchange, btc_candles)

        # self.df is the smallest interval, so this condition should cover resampled DFs as well as Heikin Ashi DF
        if not self.df.empty and self.df.close.size > 0:
            # Basic technical indicators
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.df = Indicators.moving_averages(self.df, 7)
            self.df = Indicators.moving_averages(self.df, 25)
            self.df = Indicators.moving_averages(self.df, 100)

            # Oscillators
            self.df = Indicators.macd(self.df)
            self.df = Indicators.rsi(df=self.df)

            # Advanced technicals
            self.df = Indicators.ma_spreads(self.df)
            self.df = Indicators.bollinguer_spreads(self.df)
            self.df = Indicators.set_twap(self.df)

            # correlation with BTC
            if not self.df_btc.empty and self.df_btc.close.size > 0:
                self.btc_beta, self.btc_correlation = self.dynamic_btc_beta_corr()
                df_pct_change = self.df_btc["close"].pct_change(periods=96) * 100
                self.btc_price_change = (
                    df_pct_change[-1:].iloc[0] if not df_pct_change.empty else 0.0
                )

            self.df = HeikinAshi().post_process(self.df)
            self.df_1h = HeikinAshi().post_process(self.df_1h)
            self.df_4h = HeikinAshi().post_process(self.df_4h)
            self.load_algorithms()

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df["ma_7"].size < 7
                or self.df["ma_25"].size < 25
                or self.df["ma_100"].size < 100
            ):
                return

            close_price = float(self.df["close"].iloc[-1])
            spreads = self.bb_spreads()

            await self.sh3.signal(
                current_price=close_price,
                bb_high=spreads.bb_high,
                bb_mid=spreads.bb_mid,
                bb_low=spreads.bb_low,
            )

            # Apex Flow signals
            await self.af.signal(
                current_price=close_price,
                btc_correlation=self.btc_correlation,
                btc_price_change=self.btc_price_change,
                btc_beta=self.btc_beta,
            )

        return

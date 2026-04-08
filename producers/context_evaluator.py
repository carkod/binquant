from shared.config import Config
from pandera.typing import DataFrame as TypedDataFrame
from numpy import isnan, log as logarithm, nan
from pybinbot import (
    BinanceKlineIntervals,
    ExchangeId,
    HABollinguerSpread,
    KlineSchema,
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
    MarketType,
    KucoinFutures,
)
from market_regime_prediction.models import LiveMarketContext
from market_regime_prediction.signal_context_scorer import SignalContextScorer

from algorithms.coinrule import PriceTracker
from algorithms.spike_hunter_v3_kucoin import SpikeHunterV3KuCoin
from algorithms.apex_flow import ApexFlow
from algorithms.activity_burst_pump import ActivityBurstPump
from algorithms.liquidation_sweep_pump import LiquidationSweepPump
from algorithms.top_gainers_reversal_drop import TopGainersReversalDrop
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer


class ContextEvaluator:
    def __init__(
        self,
        producer: AsyncProducer,
        api: KucoinApi | BinanceApi | KucoinFutures,
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
        binbot_api: BinbotApi,
        kucoin_symbol=None,
        market_type: MarketType = MarketType.SPOT,
        oi_data: float = None,
        latest_market_context: LiveMarketContext | None = None,
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
        self.market_type = market_type
        self.binbot_api = binbot_api
        self.symbol = symbol
        self.kucoin_symbol = kucoin_symbol
        self.df: TypedDataFrame[KlineSchema]
        self.df_15m: TypedDataFrame[KlineSchema]
        self.df_4h: TypedDataFrame[KlineSchema]
        self.df_1h: TypedDataFrame[KlineSchema]
        self.df_5m: TypedDataFrame[KlineSchema]
        self.btc_df: TypedDataFrame[KlineSchema]
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
        self.btc_beta: float = 0
        self.btc_price_change: float = 0
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
        self.oi_data = oi_data
        self.latest_market_context = latest_market_context
        self.signal_context_scorer = SignalContextScorer(
            context_weight=0.35,
            risk_weight=0.35,
            support_weight=0.2,
        )

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

        self.df_15m["returns"] = logarithm(
            self.df_15m["close"] / self.df_15m["close"].shift(1)
        )

        # Align returns
        returns = (
            self.df_15m[["returns"]]
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
        bb_high = float(self.df_15m.bb_upper.iloc[-1])
        bb_mid = float(self.df_15m.bb_mid.iloc[-1])
        bb_low = float(self.df_15m.bb_lower.iloc[-1])
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
        self.abp = ActivityBurstPump(cls=self)
        self.lsp = LiquidationSweepPump(cls=self)
        self.tgrd = TopGainersReversalDrop(cls=self)
        self.pt = PriceTracker(cls=self)

    async def process_data(
        self,
        candles,
        candles_15m=None,
        btc_candles_15m=None,
    ):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """
        self.symbol_dependent_data()
        heikin_ashi = HeikinAshi()
        candles_15m = candles_15m if candles_15m is not None else candles

        self.df, _, _, _ = heikin_ashi.pre_process(self.exchange, candles)
        self.df_15m, _, self.df_1h, self.df_4h = heikin_ashi.pre_process(
            self.exchange, candles_15m
        )
        self.df_btc, _, _, _ = heikin_ashi.pre_process(self.exchange, btc_candles_15m)

        # self.df is the smallest interval, so this condition should cover resampled DFs as well as Heikin Ashi DF
        if not self.df_15m.empty and self.df_15m.close.size > 0:
            # Basic technical indicators
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.df_15m = Indicators.moving_averages(self.df_15m, 7)
            self.df_15m = Indicators.moving_averages(self.df_15m, 25)
            self.df_15m = Indicators.moving_averages(self.df_15m, 100)

            # Oscillators
            self.df_15m = Indicators.macd(df=self.df_15m)
            self.df_15m = Indicators.rsi(df=self.df_15m)

            # Advanced technicals
            self.df_15m = Indicators.ma_spreads(self.df_15m)
            self.df_15m = Indicators.bollinguer_spreads(self.df_15m)
            self.df_15m = Indicators.set_twap(self.df_15m)

            # Default BTC-derived metrics let downstream algorithms run even
            # when benchmark candle preprocessing yields no usable rows.
            self.btc_beta = 0.0
            self.btc_correlation = 0.0
            self.btc_price_change = 0.0

            # correlation with BTC
            if not self.df_btc.empty and self.df_btc.close.size > 0:
                self.btc_beta, self.btc_correlation = self.dynamic_btc_beta_corr()
                df_pct_change = self.df_btc["close"].pct_change(periods=96) * 100
                self.btc_price_change = (
                    df_pct_change[-1:].iloc[0] if not df_pct_change.empty else 0.0
                )

            self.df = heikin_ashi.post_process(self.df)
            self.df_15m = heikin_ashi.post_process(self.df_15m)
            self.df_1h = heikin_ashi.post_process(self.df_1h)
            self.df_4h = heikin_ashi.post_process(self.df_4h)
            self.load_algorithms()

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df_15m["ma_7"].size < 7
                or self.df_15m["ma_25"].size < 25
                or self.df_15m["ma_100"].size < 100
            ):
                return

            close_price = float(self.df_15m["close"].iloc[-1])

            # below signals require spreads
            spreads = self.bb_spreads()

            await self.lsp.signal_generator(
                current_price=close_price,
                bb_high=spreads.bb_high,
                bb_mid=spreads.bb_mid,
                bb_low=spreads.bb_low,
            )

            await self.abp.signal(
                current_price=close_price,
                bb_high=spreads.bb_high,
                bb_mid=spreads.bb_mid,
                bb_low=spreads.bb_low,
            )

            await self.tgrd.signal(
                current_price=close_price,
                bb_high=spreads.bb_high,
                bb_mid=spreads.bb_mid,
                bb_low=spreads.bb_low,
            )

            await self.sh3.signal(
                current_price=close_price,
                bb_high=spreads.bb_high,
                bb_mid=spreads.bb_mid,
                bb_low=spreads.bb_low,
            )

            await self.pt.signal(
                close_price=close_price,
                bb_high=spreads.bb_high,
                bb_low=spreads.bb_low,
                bb_mid=spreads.bb_mid,
            )

            # Apex Flow signals
            await self.af.signal()

        return

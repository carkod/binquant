import logging
from datetime import datetime, UTC
from typing import Any, cast
from collections.abc import Awaitable

from numpy import isnan
from numpy import log as logarithm
from numpy import nan
from pandas import DataFrame
from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import (
    BinanceApi,
    BinanceKlineIntervals,
    BinbotApi,
    Candles,
    ExchangeId,
    HABollinguerSpread,
    Indicators,
    KlineSchema,
    KucoinApi,
    KucoinFutures,
    KucoinKlineIntervals,
    MarketDominance,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
)

from strategies.activity_burst_pump import ActivityBurstPump
from strategies.apex_flow import ApexFlow
from strategies.coinrule.buy_the_dip import BuyTheDip
from strategies.coinrule.bb_extreme_reversion import BBExtremeReversion
from strategies.coinrule.price_tracker import PriceTracker
from strategies.grid.ladder_deployer import LadderDeployer
from strategies.liquidation_sweep_pump import LiquidationSweepPump
from strategies.inverse_price_tracker import InversePriceTracker
from strategies.range_bb_rsi_mean_reversion import RangeBbRsiMeanReversion
from strategies.range_failed_breakout_fade import RangeFailedBreakoutFade
from strategies.relative_strength_reversal_range import RelativeStrengthReversalRange
from strategies.spike_hunter_v3_kucoin import SpikeHunterV3KuCoin
from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from market_regime.models import LiveMarketContext
from market_regime.signal_context_scorer import SignalContextScorer
from shared.config import Config
from shared.utils import format_context_timestamp_line


class ContextEvaluator:
    def __init__(
        self,
        api: KucoinApi | BinanceApi | KucoinFutures,
        symbol: str,
        current_symbol_data,
        market_breadth_data,
        all_symbols,
        ac_api: AutotradeConsumer,
        exchange: ExchangeId,
        first_seen_at: int,
        interval: BinanceKlineIntervals | KucoinKlineIntervals,
        binbot_api: BinbotApi,
        telegram_consumer: TelegramConsumer,
        strategy_cooldowns: dict[tuple[str, str], int] | None = None,
        kucoin_symbol=None,
        market_type: MarketType = MarketType.SPOT,
        oi_data: float = None,
        latest_market_context: LiveMarketContext | None = None,
        latest_market_context_provider=None,
        last_market_regime: str | None = None,
    ) -> None:
        """
        Only variables no data requests (third party or db)
        or pipeline instances

        Network requested data that doesn't require reloading/real-time/updating
        should be on klines_provider instance
        """
        self.api = api
        self.config = Config()
        self.market_type = market_type
        self.binbot_api = binbot_api
        self.symbol = symbol
        self.kucoin_symbol = kucoin_symbol
        self.df_5m: TypedDataFrame[KlineSchema]
        self.df_15m: TypedDataFrame[KlineSchema]
        self.df_1h: TypedDataFrame[KlineSchema]
        self.df_btc_15m: TypedDataFrame[KlineSchema]
        self.exchange = exchange
        self.interval = interval
        # describes current USDC market: gainers vs losers
        self.current_market_dominance: MarketDominance = MarketDominance.NEUTRAL
        # describes whether tide is shifting
        self.market_domination_reversal: bool = False
        self.bot_strategy: Position = Position.long
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
        self.telegram_consumer = telegram_consumer
        self.strategy_cooldowns = strategy_cooldowns
        self.at_consumer = ac_api
        # Countdown for Apex Flow score system
        self.first_seen_at = first_seen_at
        self.oi_data = oi_data
        self._latest_market_context = latest_market_context
        self._latest_market_context_provider = latest_market_context_provider
        self.last_market_regime = last_market_regime
        self.signal_context_scorer = SignalContextScorer(
            context_weight=0.35,
            risk_weight=0.35,
            support_weight=0.2,
        )
        self._breadth_cross_tolerance = 0.05
        self._autotrade_stress_threshold = 0.35

    @property
    def latest_market_context(self) -> LiveMarketContext | None:
        provider = self._latest_market_context_provider
        if provider is not None:
            return provider.latest_market_context
        return self._latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value: LiveMarketContext | None) -> None:
        self._latest_market_context = value
        provider = self._latest_market_context_provider
        if provider is not None:
            provider.latest_market_context = value

    def context_timestamp_line(self, context: LiveMarketContext | None = None) -> str:
        resolved_context = (
            context if context is not None else self.latest_market_context
        )
        return format_context_timestamp_line(resolved_context)

    def days(self, secs):
        return secs * 86400

    def dynamic_btc_beta_corr(self, window=50) -> tuple[float, float]:
        """
        Rolling beta and correlation of asset returns vs BTC returns
        Caches returns for BTC but not for the asset

        - Correlation = move
        - Beta = magnitude
        """
        if "returns" not in self.df_btc_15m:
            self.df_btc_15m["returns"] = logarithm(
                self.df_btc_15m["close"] / self.df_btc_15m["close"].shift(1)
            )

        self.df_15m["returns"] = logarithm(
            self.df_15m["close"] / self.df_15m["close"].shift(1)
        )

        # Align returns
        returns = (
            self.df_15m[["returns"]]
            .join(self.df_btc_15m["returns"], how="inner", rsuffix="_btc")
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

    def bb_spreads(self, df: TypedDataFrame[KlineSchema]) -> HABollinguerSpread:
        """
        Calculate Bollinger band spreads for trailing strategies.

        This is mainly used to set autotrade bots initial take profit and stop loss levels
        """
        bb_high = float(df.bb_upper.iloc[-1])
        bb_mid = float(df.bb_mid.iloc[-1])
        bb_low = float(df.bb_lower.iloc[-1])
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

    def load_5m_algorithms(self):
        """
        Initialize algorithms that consume self.df_5m data.
        """
        self.abp = ActivityBurstPump(cls=self)
        self.pt = PriceTracker(cls=self)
        self.ipt = InversePriceTracker(cls=self)

    def load_15m_algorithms(self):
        """
        Initialize algorithms that consume self.df_15m and broader market context.
        """
        self.sh3 = SpikeHunterV3KuCoin(cls=self)
        self.af = ApexFlow(cls=self)
        self.lsp = LiquidationSweepPump(cls=self)
        self.bbex = BBExtremeReversion(cls=self)
        self.coinrule_buy_the_dip = BuyTheDip(cls=self)
        self.rbrmr = RangeBbRsiMeanReversion(cls=self)
        self.rfbf = RangeFailedBreakoutFade(cls=self)
        self.rsrr = RelativeStrengthReversalRange(cls=self)
        self.grid_ladder = LadderDeployer(cls=self)

    def indicators_enrichment(
        self, df: TypedDataFrame[KlineSchema]
    ) -> TypedDataFrame[KlineSchema]:
        """
        Enrich dataframe with technical indicators

        This would be an ideal process to spark.parallelize
        not sure what's the best way with pandas-on-spark dataframe
        """
        df = Indicators.moving_averages(df, 7)
        df = Indicators.moving_averages(df, 25)
        df = Indicators.moving_averages(df, 100)

        # Oscillators
        df = Indicators.macd(df=df)
        df = Indicators.rsi(df=df)

        # Advanced technicals
        df = Indicators.ma_spreads(df)
        df = Indicators.bollinguer_spreads(df)
        df = Indicators.set_twap(df)

        return df

    async def _safe_signal(self, name: str, coro: Awaitable[None]) -> None:
        """
        Run a single strategy's signal coroutine with crash isolation: any
        exception is logged and swallowed so one bad strategy can't take down
        the entire pipeline for the current kline.
        """
        try:
            await coro
        except Exception:
            logging.exception(
                "Strategy %s raised while processing %s; continuing.",
                name,
                self.symbol,
            )

    def dispatch_signal_record(
        self,
        value: SignalsConsumer,
        indicators: dict[str, Any] | None = None,
    ) -> None:
        """
        Fire-and-forget POST /signals so every strategy emission lands in the
        analytics table, regardless of whether autotrade actually fires.

        Wrapped in a defensive try/except: an analytics-write failure must
        never abort the trading path. HTTP errors are already handled inside
        BinbotApi.create_signal; this guard covers the synchronous prep work
        (model_dump, attribute access) so a malformed payload can't bubble
        up and short-circuit telegram/autotrade dispatch for this kline.
        """
        grid_params = value.grid_params
        signal_kind = value.signal_kind
        try:
            signal_kind = value.signal_kind or "bot"
            bot_params = value.bot_params
            grid_params = value.grid_params

            context = self.latest_market_context
            if bot_params is not None:
                position = bot_params.position
                direction = (
                    position.value
                    if hasattr(position, "value")
                    else (
                        position
                        if position is not None
                        else value.direction or "UNKNOWN"
                    )
                )
            else:
                direction = value.direction or "grid"
            regime = (
                context.market_regime if context and context.market_regime else None
            )

            merged_indicators: dict[str, Any] = dict(indicators or {})
            if value.bb_spreads is not None:
                merged_indicators.setdefault(
                    "bb_spreads", value.bb_spreads.model_dump(mode="json")
                )
            if value.current_price:
                merged_indicators.setdefault("current_price", value.current_price)
            if value.score:
                merged_indicators.setdefault("score", value.score)

            self.binbot_api.dispatch_create_signal(
                algorithm_name=(
                    bot_params.name if bot_params is not None else "grid_ladder"
                ),
                symbol=self.symbol,
                generated_at=datetime.now(UTC),
                direction=direction,
                autotrade=value.autotrade,
                current_regime=regime,
                context=context.model_dump(mode="json") if context else {},
                signal_kind=signal_kind,
                bot_params=(bot_params.model_dump(mode="json") if bot_params else {}),
                grid_params=(
                    grid_params.model_dump(mode="json") if grid_params else {}
                ),
                indicators=merged_indicators,
            )
        except Exception:
            logging.exception(
                "dispatch_signal_record failed for %s; trade path continues.",
                self.symbol,
            )

    async def process_data(
        self,
        candles,
        candles_15m,
        btc_candles_15m=None,
    ):
        """
        Create all the dataframes needed for the strategies
        - Raw candles 5m
        - Raw candles 15m
        - Raw candles 1h resampled from 15m
        - Raw BTC candles 15m

        Algorithms should consume this data
        """
        self.symbol_dependent_data()
        raw_candles_5m = Candles(exchange=self.exchange, candles=candles)
        raw_candles_15m = Candles(exchange=self.exchange, candles=candles_15m)

        self.df_5m = raw_candles_5m.pre_process()
        if not self.df_5m.empty and self.df_5m.close.size > 0:
            self.load_5m_algorithms()
            self.df_5m = self.indicators_enrichment(self.df_5m)
            self.df_5m = raw_candles_5m.post_process(self.df_5m)

            if (
                self.df_5m.ma_7.size < 7
                or self.df_5m.ma_25.size < 25
                or self.df_5m.ma_100.size < 100
            ):
                return

            close_price = float(self.df_5m["close"].iloc[-1])
            spreads = self.bb_spreads(self.df_5m)

            await self._safe_signal(
                "ActivityBurstPump",
                self.abp.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )

            await self._safe_signal(
                "PriceTracker",
                self.pt.signal(
                    close_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_low=spreads.bb_low,
                    bb_mid=spreads.bb_mid,
                ),
            )

            await self._safe_signal(
                "InversePriceTracker",
                self.ipt.signal(
                    close_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_low=spreads.bb_low,
                    bb_mid=spreads.bb_mid,
                ),
            )

        self.df_15m = raw_candles_15m.pre_process()
        self.df_1h = cast(
            TypedDataFrame[KlineSchema],
            raw_candles_15m.resample(self.df_15m, interval="1h"),
        )

        if not self.df_15m.empty and self.df_15m.close.size > 0:
            self.load_15m_algorithms()
            self.df_btc_15m = (
                Candles(exchange=self.exchange, candles=btc_candles_15m).pre_process()
                if btc_candles_15m
                else cast(TypedDataFrame[KlineSchema], DataFrame())
            )
            self.df_15m = self.indicators_enrichment(self.df_15m)

            # Default BTC-derived metrics let downstream algorithms run even
            # when benchmark candle preprocessing yields no usable rows.
            self.btc_beta = 0.0
            self.btc_correlation = 0.0
            self.btc_price_change = 0.0

            # correlation with BTC
            if not self.df_btc_15m.empty and self.df_btc_15m.close.size > 0:
                self.btc_beta, self.btc_correlation = self.dynamic_btc_beta_corr()
                df_pct_change = self.df_btc_15m["close"].pct_change(periods=96) * 100
                self.btc_price_change = (
                    df_pct_change[-1:].iloc[0] if not df_pct_change.empty else 0.0
                )

            self.df_15m = raw_candles_15m.post_process(self.df_15m)
            self.df_1h = raw_candles_15m.post_process(self.df_1h)

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df_15m["ma_7"].size < 7
                or self.df_15m["ma_25"].size < 25
                or self.df_15m["ma_100"].size < 100
            ):
                return

            close_price = float(self.df_15m["close"].iloc[-1])
            spreads = self.bb_spreads(self.df_15m)

            await self._safe_signal("ApexFlow", self.af.signal())
            self.last_market_regime = self.af.last_market_regime

            # Keep 15m entry strategies ordered from rarest/selective to broadest.
            # The first matching autotrade setup gets the cleanest chance to fire.
            await self._safe_signal(
                "RangeFailedBreakoutFade",
                self.rfbf.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )
            await self._safe_signal(
                "RangeBbRsiMeanReversion",
                self.rbrmr.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )
            await self._safe_signal(
                "RelativeStrengthReversalRange",
                self.rsrr.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )
            await self._safe_signal(
                "LiquidationSweepPump",
                self.lsp.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )
            await self._safe_signal(
                "SpikeHunterV3KuCoin",
                self.sh3.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )

            await self._safe_signal(
                "LadderDeployer",
                self.grid_ladder.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )

            # Disabled temporarily to test grid ladder
            await self._safe_signal(
                "BuyTheDip",
                self.coinrule_buy_the_dip.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )

            await self._safe_signal(
                "BBExtremeReversion",
                self.bbex.signal(
                    current_price=close_price,
                    bb_high=spreads.bb_high,
                    bb_mid=spreads.bb_mid,
                    bb_low=spreads.bb_low,
                ),
            )

        return

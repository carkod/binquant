from __future__ import annotations

import logging
from asyncio import timeout
from collections.abc import Awaitable
from datetime import UTC, datetime
from time import time
from typing import TYPE_CHECKING, Any, Literal, cast

from numpy import isnan
from numpy import log as logarithm
from numpy import nan
from pandas import DataFrame, Series, to_numeric
from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import (
    BinanceApi,
    BinanceKlineIntervals,
    BinbotApi,
    BotModel,
    Candles,
    ExchangeId,
    HABollinguerSpread,
    Indicators,
    KlineSchema,
    KucoinApi,
    KucoinFutures,
    KucoinKlineIntervals,
    GainersLosersSnapshot,
    MarketBreadthSeries,
    MarketDominance,
    MarketType,
    Position,
    SignalsConsumer,
    SymbolModel,
    round_numbers,
)

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.telegram_consumer import TelegramConsumer
from market_regime.grid_only_policy import GridOnlyPolicy
from market_regime.models import LiveMarketContext
from market_regime.open_interest_order_sizing import (
    OI_SIZED_STRATEGIES,
    apply_open_interest_sizing,
)
from market_regime.signal_context_scorer import SignalContextScorer
from shared.config import Config
from shared.macroregime_directional_notifier import MacroregimeDirectionalNotifier
from shared.utils import format_context_timestamp_line
from strategies.higher_low_pattern import HigherLowPattern
from strategies.liquidation_sweep_pump import LiquidationSweepPortfolioSelector
from strategies.lower_high_pattern import LowerHighPattern
from strategies.top_gainer_breadth import TopGainerBreadth
from strategies.top_gainer_early_momentum import TopGainerEarlyMomentum
from strategies.top_loser_breadth import TopLoserBreadth
from strategies.top_loser_early_momentum import TopLoserEarlyMomentum

if TYPE_CHECKING:
    from strategies.activity_burst.activity_burst_anomaly_gate import (
        ActivityBurstAnomalyGate,
    )


class ContextEvaluator:
    SIGNAL_PERSISTENCE_TIMEOUT_SECONDS = 2.0
    REGIME_WINDOW_BARS = 96
    REGIME_MINIMUM_BARS = 96
    DIRECTIONAL_EFFICIENCY_THRESHOLD = 0.35
    OSCILLATION_FULL_INTENSITY_TRAVEL = 0.08

    def __init__(
        self,
        api: KucoinApi | BinanceApi | KucoinFutures,
        symbol: str,
        current_symbol_data: SymbolModel,
        market_breadth_data: MarketBreadthSeries | None,
        gainers_losers_series: list[GainersLosersSnapshot],
        all_symbols: list[SymbolModel],
        ac_api: AutotradeConsumer,
        exchange: ExchangeId,
        first_seen_at: int,
        interval: BinanceKlineIntervals | KucoinKlineIntervals,
        binbot_api: BinbotApi,
        telegram_consumer: TelegramConsumer,
        strategy_cooldowns: dict[tuple[str, str], int] | None = None,
        strategy_states: dict[tuple[str, str], dict[str, float | int]] | None = None,
        activity_burst_anomaly_gates: dict[str, ActivityBurstAnomalyGate] | None = None,
        liquidation_sweep_portfolio_selector: (
            LiquidationSweepPortfolioSelector | None
        ) = None,
        kucoin_symbol=None,
        market_type: MarketType = MarketType.SPOT,
        latest_market_context: LiveMarketContext | None = None,
        last_macroregime_directional: str | None = None,
        top_gainer_recovery_bots: list[BotModel] | None = None,
        top_gainer_recovery_attempted_source_ids: set[str] | None = None,
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
        self.gainers_losers_series = gainers_losers_series
        self.btc_correlation: float = 0
        self.btc_beta: float = 0
        self.btc_price_change: float = 0
        self.repeated_signals: dict = {}
        self.all_symbols = all_symbols
        # theorically current_symbol_data is always defined
        # if it's not defined, then it wouldn't subscribe with websockets
        self.current_symbol_data = current_symbol_data
        self.price_precision = self.current_symbol_data.price_precision
        self.telegram_consumer = telegram_consumer
        self.strategy_cooldowns = strategy_cooldowns
        self.strategy_states = strategy_states if strategy_states is not None else {}
        self.activity_burst_anomaly_gates = (
            activity_burst_anomaly_gates
            if activity_burst_anomaly_gates is not None
            else {}
        )
        self.top_gainer_recovery_bots = top_gainer_recovery_bots or []
        self.top_gainer_recovery_attempted_source_ids = (
            top_gainer_recovery_attempted_source_ids
            if top_gainer_recovery_attempted_source_ids is not None
            else set()
        )
        self.liquidation_sweep_portfolio_selector = liquidation_sweep_portfolio_selector
        self.at_consumer = ac_api
        # Countdown for Apex Flow score system
        self.first_seen_at = first_seen_at
        self.latest_market_context = latest_market_context
        self.macroregime_directional: Literal["UP", "DOWN", "NONE"] | None = None
        self.macroregime_oscillation_intensity: float | None = None
        self.microregime_directional: Literal["UP", "DOWN", "NONE"] | None = None
        self.microregime_oscillation_intensity: float | None = None
        self.last_macroregime_directional = last_macroregime_directional
        self.grid_only_policy = GridOnlyPolicy.disabled("not_evaluated")
        self.at_consumer.grid_only_policy = self.grid_only_policy
        self.signal_context_scorer = SignalContextScorer(
            context_weight=0.35,
            risk_weight=0.35,
            support_weight=0.2,
        )
        self._breadth_cross_tolerance = 0.05
        self._autotrade_stress_threshold = 0.35

    def refresh_grid_only_policy(self) -> GridOnlyPolicy:
        if not self.at_consumer.autotrade_settings.enable_grid_ladders:
            self.grid_only_policy = GridOnlyPolicy.disabled("grid_ladders_disabled")
        else:
            self.grid_only_policy = GridOnlyPolicy.resolve(
                self.latest_market_context,
                self.market_breadth_data,
            )
        self.at_consumer.grid_only_policy = self.grid_only_policy
        return self.grid_only_policy

    def context_timestamp_line(self, context: LiveMarketContext | None = None) -> str:
        resolved_context = (
            context if context is not None else self.latest_market_context
        )
        return format_context_timestamp_line(resolved_context)

    @classmethod
    def assess_directional_and_oscillation(
        cls,
        candles: DataFrame,
    ) -> tuple[Literal["UP", "DOWN", "NONE"] | None, float | None]:
        """Measure trend direction and non-trending movement over 24 hours.

        Direction is based only on path efficiency: the absolute start-to-end
        move divided by all close-to-close movement. Oscillation intensity is
        the inefficient portion of that path, scaled by total movement so a
        quiet flat market is not mistaken for an active oscillating market.
        """
        if "close" not in candles:
            return None, None

        closes = (
            candles["close"]
            .astype(float)
            .replace([float("inf"), float("-inf")], nan)
            .dropna()
            .tail(cls.REGIME_WINDOW_BARS)
        )
        if len(closes) < cls.REGIME_MINIMUM_BARS or (closes <= 0).any():
            return None, None

        log_closes = Series(logarithm(closes.to_numpy()), index=closes.index)
        log_moves = log_closes.diff().dropna()
        gross_travel = float(log_moves.abs().sum())
        if gross_travel == 0:
            return "NONE", 0.0

        net_move = float(log_closes.iloc[-1] - log_closes.iloc[0])
        path_efficiency = min(abs(net_move) / gross_travel, 1.0)
        directional: Literal["UP", "DOWN", "NONE"] = "NONE"
        if path_efficiency >= cls.DIRECTIONAL_EFFICIENCY_THRESHOLD:
            directional = "UP" if net_move > 0 else "DOWN"

        movement_intensity = min(
            gross_travel / cls.OSCILLATION_FULL_INTENSITY_TRAVEL,
            1.0,
        )
        oscillation_intensity = round(
            movement_intensity * (1.0 - path_efficiency),
            3,
        )
        return directional, oscillation_intensity

    def refresh_regime_measures(self) -> None:
        """Apply the regime calculation to completed BTC and asset candles."""
        evaluation_time_ms = time() * 1000
        completed_btc_candles = self._completed_regime_candles(
            self.df_btc_15m,
            evaluation_time_ms,
        )
        completed_symbol_candles = self._completed_regime_candles(
            self.df_15m,
            evaluation_time_ms,
        )
        (
            self.macroregime_directional,
            self.macroregime_oscillation_intensity,
        ) = self.assess_directional_and_oscillation(completed_btc_candles)
        (
            self.microregime_directional,
            self.microregime_oscillation_intensity,
        ) = self.assess_directional_and_oscillation(completed_symbol_candles)

    @staticmethod
    def _completed_regime_candles(
        candles: DataFrame,
        evaluation_time_ms: float,
    ) -> DataFrame:
        if "close_time" not in candles:
            return candles.iloc[0:0]
        close_times = to_numeric(candles["close_time"], errors="coerce")
        return candles.loc[close_times < evaluation_time_ms]

    def regime_measures(self) -> dict[str, str | float | None]:
        """Return the shared macro/micro regime payload used by strategies."""
        return {
            "macroregime_directional": self.macroregime_directional,
            "macroregime_oscillation_intensity": (
                self.macroregime_oscillation_intensity
            ),
            "microregime_directional": self.microregime_directional,
            "microregime_oscillation_intensity": (
                self.microregime_oscillation_intensity
            ),
        }

    def regime_telegram_lines(self) -> str:
        """Render the four shared regime measures for strategy notifications."""

        def display(value: str | float | None) -> str:
            if value is None:
                return "UNAVAILABLE"
            if isinstance(value, float):
                return str(round_numbers(value, 3))
            return value

        return "\n".join(
            (
                f"- Macro directional (BTC): {display(self.macroregime_directional)}",
                "- Macro oscillation intensity (BTC): "
                f"{display(self.macroregime_oscillation_intensity)}",
                f"- Micro directional ({self.symbol}): "
                f"{display(self.microregime_directional)}",
                f"- Micro oscillation intensity ({self.symbol}): "
                f"{display(self.microregime_oscillation_intensity)}",
            )
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
        self.current_symbol_data = [s for s in self.all_symbols if s.id == self.symbol][
            0
        ]
        self.price_precision = self.current_symbol_data.price_precision
        self.qty_precision = self.current_symbol_data.qty_precision

    def load_15m_algorithms(self):
        """
        Initialize the temporarily enabled 15m algorithms.
        """
        self.macroregime_directional_notifier = MacroregimeDirectionalNotifier(cls=self)
        self.top_gainer_breadth = TopGainerBreadth(cls=self)
        self.top_loser_breadth = TopLoserBreadth(cls=self)
        self.top_gainer_early_momentum = TopGainerEarlyMomentum(cls=self)
        self.top_loser_early_momentum = TopLoserEarlyMomentum(cls=self)
        self.lower_high_pattern = LowerHighPattern(cls=self)
        self.higher_low_pattern = HigherLowPattern(cls=self)

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
        df = Indicators.atr(df=df, window=14)

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

    def finalize_signal_bot_params(self, value: SignalsConsumer) -> None:
        """Finalize context-derived bot parameters before persistence or execution."""
        bot_params = value.bot_params
        if (
            bot_params is None
            or bot_params.market_type != MarketType.FUTURES
            or bot_params.name not in OI_SIZED_STRATEGIES
        ):
            return

        context = self.latest_market_context
        symbol_features = (
            context.get_symbol_features(self.symbol) if context is not None else None
        )
        value.open_interest_sizing = apply_open_interest_sizing(
            bot_params=bot_params,
            positioning=(
                symbol_features.derivatives if symbol_features is not None else None
            ),
            signal_timestamp=int(datetime.now(UTC).timestamp() * 1000),
        )

    async def dispatch_signal_record(
        self,
        value: SignalsConsumer,
        indicators: dict[str, Any] | None = None,
    ) -> None:
        """
        Persist every strategy emission and attach its database ID to any bot
        or grid payload before autotrade dispatches it.
        """
        grid_params = value.grid_params
        signal_kind = value.signal_kind
        try:
            signal_kind = value.signal_kind or "bot"
            bot_params = value.bot_params
            grid_params = value.grid_params

            context = self.latest_market_context
            symbol_features = (
                context.get_symbol_features(self.symbol)
                if context is not None
                else None
            )
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
            regime_value = self.regime_measures()["macroregime_directional"]
            regime = regime_value if isinstance(regime_value, str) else None

            merged_indicators: dict[str, Any] = dict(indicators or {})
            if bot_params is not None and bot_params.market_type == MarketType.FUTURES:
                merged_indicators.setdefault(
                    "estimated_initial_margin",
                    bot_params.fiat_order_size,
                )
            if value.open_interest_sizing is not None:
                merged_indicators.setdefault(
                    "open_interest_sizing",
                    value.open_interest_sizing.model_dump(mode="json"),
                )
            if symbol_features is not None and symbol_features.derivatives is not None:
                merged_indicators.setdefault(
                    "derivatives_positioning",
                    symbol_features.derivatives.model_dump(mode="json"),
                )
            if value.bb_spreads is not None:
                merged_indicators.setdefault(
                    "bb_spreads", value.bb_spreads.model_dump(mode="json")
                )
            if value.current_price:
                merged_indicators.setdefault("current_price", value.current_price)
            if value.score:
                merged_indicators.setdefault("score", value.score)
            for key, regime_value in self.regime_measures().items():
                merged_indicators.setdefault(key, regime_value)

            async with timeout(self.SIGNAL_PERSISTENCE_TIMEOUT_SECONDS):
                signal = await self.binbot_api.create_signal(
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
                    bot_params=(
                        bot_params.model_dump(mode="json") if bot_params else {}
                    ),
                    grid_params=(
                        grid_params.model_dump(mode="json") if grid_params else {}
                    ),
                    indicators=merged_indicators,
                )
            if signal is None or signal.id is None:
                return
            if grid_params is not None:
                grid_params.signal_id = signal.id
            if bot_params is not None:
                bot_params.signal_id = signal.id
        except TimeoutError:
            logging.warning(
                "Signal persistence for %s exceeded %.1fs; "
                "trade path continues without signal_id.",
                self.symbol,
                self.SIGNAL_PERSISTENCE_TIMEOUT_SECONDS,
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
        candles_1h=None,
        btc_candles_15m=None,
    ):
        """
        Create all the dataframes needed for the strategies
        - Raw candles 5m
        - Raw candles 15m
        - Raw candles 1h, falling back to 15m resampling for older callers
        - Raw BTC candles 15m

        Algorithms should consume this data
        """
        # Reserve staging for FailedSpikeFade validation. The remaining trading
        # strategies run only in production.
        run_production_strategies = self.config.env.casefold() == "production"
        self.symbol_dependent_data()
        self.refresh_grid_only_policy()
        raw_candles_5m = Candles(exchange=self.exchange, candles=candles)
        raw_candles_15m = Candles(exchange=self.exchange, candles=candles_15m)
        raw_candles_1h = (
            Candles(exchange=self.exchange, candles=candles_1h)
            if candles_1h is not None
            else None
        )

        self.df_5m = raw_candles_5m.pre_process()
        if not self.df_5m.empty and self.df_5m.close.size > 0:
            self.df_5m = self.indicators_enrichment(self.df_5m)
            self.df_5m = raw_candles_5m.post_process(self.df_5m)

        self.df_15m = raw_candles_15m.pre_process()
        self.df_1h = cast(
            TypedDataFrame[KlineSchema],
            (
                raw_candles_1h.pre_process()
                if raw_candles_1h is not None
                else raw_candles_15m.resample(self.df_15m, interval="1h")
            ),
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

            self.refresh_regime_measures()

            self.df_15m = raw_candles_15m.post_process(self.df_15m)
            self.df_1h = (
                raw_candles_1h.post_process(self.df_1h)
                if raw_candles_1h is not None
                else raw_candles_15m.post_process(self.df_1h)
            )

            # Dropped NaN values may end up with empty dataframe
            if (
                self.df_15m["ma_7"].size < 7
                or self.df_15m["ma_25"].size < 25
                or self.df_15m["ma_100"].size < 100
            ):
                return

            close_price = float(self.df_15m["close"].iloc[-1])
            spreads = self.bb_spreads(self.df_15m)

            if run_production_strategies:
                await self._safe_signal(
                    "TopGainerBreadth",
                    self.top_gainer_breadth.signal(
                        current_price=close_price,
                        bb_high=spreads.bb_high,
                        bb_mid=spreads.bb_mid,
                        bb_low=spreads.bb_low,
                    ),
                )

                await self._safe_signal(
                    "TopLoserBreadth",
                    self.top_loser_breadth.signal(
                        current_price=close_price,
                        bb_high=spreads.bb_high,
                        bb_mid=spreads.bb_mid,
                        bb_low=spreads.bb_low,
                    ),
                )

                await self._safe_signal(
                    "TopGainerEarlyMomentum",
                    self.top_gainer_early_momentum.signal(
                        current_price=close_price,
                        bb_high=spreads.bb_high,
                        bb_mid=spreads.bb_mid,
                        bb_low=spreads.bb_low,
                    ),
                )

                await self._safe_signal(
                    "TopLoserEarlyMomentum",
                    self.top_loser_early_momentum.signal(
                        current_price=close_price,
                        bb_high=spreads.bb_high,
                        bb_mid=spreads.bb_mid,
                        bb_low=spreads.bb_low,
                    ),
                )

            await self._safe_signal(
                "MacroregimeDirectionalNotifier",
                self.macroregime_directional_notifier.signal(),
            )
            self.last_macroregime_directional = (
                self.macroregime_directional_notifier.last_macroregime_directional
            )

            await self._safe_signal(
                "LowerHighPattern",
                self.lower_high_pattern.signal(),
            )

            await self._safe_signal(
                "HigherLowPattern",
                self.higher_low_pattern.signal(),
            )

        return

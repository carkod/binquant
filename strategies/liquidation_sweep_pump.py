import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING, Any

import numpy as np
from pandas import DataFrame, concat
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    KlineSchema,
    MarketBreadthSeries,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
    timestamp_sort_key,
)
from pandera.typing import DataFrame as TypedDataFrame
from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


@dataclass(frozen=True)
class LiquidationSweepCandidate:
    candle_open_time: int
    symbol: str
    rank_score: float
    dispatch: Callable[[], Awaitable[None]]


class LiquidationSweepPortfolioSelector:
    """Select one cross-symbol liquidation-sweep candidate per candle cohort."""

    def __init__(self) -> None:
        self._latest_candle_open_time: int | None = None
        self._candidates: dict[int, dict[str, LiquidationSweepCandidate]] = {}

    async def observe(self, candle_open_time: int) -> None:
        latest = self._latest_candle_open_time
        if latest is not None and candle_open_time <= latest:
            return

        for completed_candle in sorted(self._candidates):
            if completed_candle >= candle_open_time:
                break
            await self._dispatch_winner(completed_candle)
        self._latest_candle_open_time = candle_open_time

    async def submit(self, candidate: LiquidationSweepCandidate) -> bool:
        await self.observe(candidate.candle_open_time)
        if (
            self._latest_candle_open_time is not None
            and candidate.candle_open_time < self._latest_candle_open_time
        ):
            return False

        cohort = self._candidates.setdefault(candidate.candle_open_time, {})
        current = cohort.get(candidate.symbol)
        if current is None or candidate.rank_score >= current.rank_score:
            cohort[candidate.symbol] = candidate
        return True

    async def flush(self) -> None:
        for candle_open_time in sorted(self._candidates):
            await self._dispatch_winner(candle_open_time)

    async def _dispatch_winner(self, candle_open_time: int) -> None:
        cohort = self._candidates.pop(candle_open_time, {})
        if not cohort:
            return
        winner = max(
            cohort.values(),
            key=lambda candidate: (candidate.rank_score, candidate.symbol),
        )
        try:
            await winner.dispatch()
        except Exception:
            logging.exception(
                "Liquidation sweep portfolio winner failed for %s.", winner.symbol
            )


class LiquidationSweepPump:
    ALGO = "liquidation_sweep_pump"
    LONG_MARKET_BREADTH_THRESHOLD = -0.2
    MIN_MARKET_BREADTH_RECOVERY = 0.02
    MOMENTUM_BARS = 3
    COMPRESSION_BARS = 6
    SCORE_LOOKBACK = 48
    SCORE_QUANTILE = 0.80
    VOLUME_LOOKBACK = 20
    MIN_MOMENTUM_ATR = 0.75
    MAX_MOMENTUM_ATR = 3.0
    MIN_CLOSE_LOCATION = 0.70
    LONG_STOP_LOSS_PCT = 2.0
    LONG_TAKE_PROFIT_PCT = 2.5
    ENTRY_COOLDOWN_MINUTES = 60
    FULL_OI_CONTRACTION = 0.01
    FULL_LIQUIDATION_INTENSITY = 0.10
    FULL_NEGATIVE_ANNUALIZED_FUNDING = 0.25
    FULL_NEGATIVE_BASIS_BPS = 25.0
    MAX_POSITIONING_RANK_BONUS = 0.20

    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.config = cls.config
        # Symbol / context
        self.symbol = cls.symbol
        self.exchange = cls.exchange
        self.telegram_consumer = cls.telegram_consumer
        self.market_type = cls.market_type
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.market_breadth_data: MarketBreadthSeries | None = cls.market_breadth_data
        self.portfolio_selector: LiquidationSweepPortfolioSelector | None = getattr(
            cls, "liquidation_sweep_portfolio_selector", None
        )

    @staticmethod
    def _context_market_breadth(context: LiveMarketContext) -> float:
        return context.advancers_ratio - context.decliners_ratio

    @staticmethod
    def _coerce_market_breadth(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if isfinite(parsed) else None

    def _market_breadth_values(self, context: LiveMarketContext) -> list[float]:
        breadth = self.market_breadth_data
        if breadth is not None and len(breadth.market_breadth) >= 2:
            timestamped_values = []
            for timestamp, value in zip(
                breadth.timestamp, breadth.market_breadth, strict=False
            ):
                sort_key = timestamp_sort_key(timestamp)
                parsed = self._coerce_market_breadth(value)
                if sort_key is not None and parsed is not None:
                    timestamped_values.append((sort_key, parsed))

            if len(timestamped_values) >= 2:
                return [
                    value
                    for _, value in sorted(timestamped_values, key=lambda item: item[0])
                ]

        return [self._context_market_breadth(context)]

    def _latest_market_breadth(self, context: LiveMarketContext) -> float:
        return self._market_breadth_values(context)[-1]

    def _market_breadth_recovery(self, context: LiveMarketContext) -> float | None:
        values = self._market_breadth_values(context)
        if len(values) < 3:
            return None
        return values[-1] - values[-3]

    def long_entry_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
        btc_momentum: float,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"

        if context.market_stress_score >= 0.35:
            return False, "market_stress_too_high"

        market_breadth = self._latest_market_breadth(context)

        if market_breadth > self.LONG_MARKET_BREADTH_THRESHOLD:
            return False, "market_breadth_not_washed_out"

        market_breadth_recovery = self._market_breadth_recovery(context)
        if (
            market_breadth_recovery is None
            or market_breadth_recovery < self.MIN_MARKET_BREADTH_RECOVERY
        ):
            return False, "market_breadth_not_recovering"
        if btc_momentum <= 0:
            return False, "btc_not_increasing"
        if symbol_features is None:
            return False, "symbol_regime_unavailable"
        if symbol_features.trend_score <= 0:
            return False, "symbol_trend_not_up"
        return True, "market_breadth_recovering_btc_up_symbol_up"

    @staticmethod
    def _unit_score(value: float) -> float:
        return max(0.0, min(value, 1.0))

    def _positioning_evidence(
        self,
        symbol_features: SymbolMarketFeatures | None,
    ) -> dict[str, float | int]:
        positioning = symbol_features.derivatives if symbol_features else None
        if positioning is None:
            return {
                "positioning_available": 0,
                "oi_contraction_score": 0.0,
                "short_liquidation_dominance_score": 0.0,
                "liquidation_intensity_score": 0.0,
                "negative_funding_score": 0.0,
                "negative_basis_score": 0.0,
                "positioning_evidence_score": 0.0,
                "positioning_rank_multiplier": 1.0,
            }

        oi_changes = (
            (0.50, positioning.oi_change_5m),
            (0.30, positioning.oi_change_15m),
            (0.20, positioning.oi_change_1h),
        )
        available_oi_scores = [
            (weight, self._unit_score(-change / self.FULL_OI_CONTRACTION))
            for weight, change in oi_changes
            if change is not None
        ]
        oi_weight = sum(weight for weight, _ in available_oi_scores)
        oi_contraction_score = (
            sum(weight * score for weight, score in available_oi_scores) / oi_weight
            if oi_weight > 0
            else 0.0
        )

        total_liquidations = (
            positioning.long_liquidation_notional
            + positioning.short_liquidation_notional
        )
        short_liquidation_dominance_score = 0.0
        if total_liquidations > 0:
            short_share = positioning.short_liquidation_notional / total_liquidations
            short_liquidation_dominance_score = self._unit_score(
                (short_share - 0.5) / 0.5
            )

        liquidation_intensity_score = self._unit_score(
            (positioning.liquidation_intensity or 0.0) / self.FULL_LIQUIDATION_INTENSITY
        )
        funding_rate_score = self._unit_score(
            -(positioning.annualized_funding_rate or 0.0)
            / self.FULL_NEGATIVE_ANNUALIZED_FUNDING
        )
        funding_percentile_score = (
            self._unit_score((0.5 - positioning.funding_percentile) / 0.5)
            if positioning.funding_percentile is not None
            else 0.0
        )
        negative_funding_score = max(
            funding_rate_score,
            funding_percentile_score,
        )
        negative_basis_score = self._unit_score(
            -(positioning.mark_index_basis_bps or 0.0) / self.FULL_NEGATIVE_BASIS_BPS
        )

        evidence_score = self._unit_score(
            0.40 * oi_contraction_score
            + 0.30 * short_liquidation_dominance_score
            + 0.15 * liquidation_intensity_score
            + 0.10 * negative_funding_score
            + 0.05 * negative_basis_score
        )
        return {
            "positioning_available": 1,
            "oi_contraction_score": oi_contraction_score,
            "short_liquidation_dominance_score": short_liquidation_dominance_score,
            "liquidation_intensity_score": liquidation_intensity_score,
            "negative_funding_score": negative_funding_score,
            "negative_basis_score": negative_basis_score,
            "positioning_evidence_score": evidence_score,
            "positioning_rank_multiplier": (
                1 + self.MAX_POSITIONING_RANK_BONUS * evidence_score
            ),
        }

    def compute_pump_score(
        self,
        df: TypedDataFrame[KlineSchema],
        df_btc: TypedDataFrame[KlineSchema],
    ) -> TypedDataFrame[KlineSchema]:
        result = df.copy()
        close = result["close"].astype(float)
        high = result["high"].astype(float)
        low = result["low"].astype(float)
        volume = result["volume"].astype(float)

        previous_close = close.shift(1)
        true_range = concat(
            [
                high - low,
                (high - previous_close).abs(),
                (low - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        result["candidate_atr"] = true_range.ewm(
            alpha=1 / 14, adjust=False, min_periods=14
        ).mean()
        result["momentum_3"] = close.pct_change(self.MOMENTUM_BARS)
        result["relative_volume"] = (
            volume / volume.shift(1).rolling(self.VOLUME_LOOKBACK).mean()
        )
        result["pre_breakout_compression"] = (
            high.shift(1).rolling(self.COMPRESSION_BARS).max()
            - low.shift(1).rolling(self.COMPRESSION_BARS).min()
        ) / close.shift(1)
        result["pump_score"] = (
            result["relative_volume"]
            * result["momentum_3"].clip(lower=0)
            / result["pre_breakout_compression"].replace(0, np.nan)
        )
        result["score_threshold"] = (
            result["pump_score"]
            .shift(1)
            .rolling(self.SCORE_LOOKBACK)
            .quantile(self.SCORE_QUANTILE)
        )
        result["score_cross"] = (result["pump_score"] >= result["score_threshold"]) & (
            result["pump_score"].shift(1) < result["score_threshold"].shift(1)
        )
        result["volume_threshold"] = (
            result["relative_volume"]
            .shift(1)
            .rolling(self.SCORE_LOOKBACK)
            .quantile(self.SCORE_QUANTILE)
        )
        result["prior_high"] = high.shift(1).rolling(self.COMPRESSION_BARS).max()
        result["close_location"] = (close - low) / (high - low).replace(0, np.nan)
        result["ema20"] = close.ewm(span=20, adjust=False).mean()
        result["ema50"] = close.ewm(span=50, adjust=False).mean()
        result["trend_score"] = (result["ema20"] - result["ema50"]) / result["ema50"]
        result["momentum_atr"] = result["momentum_3"] / (
            result["candidate_atr"] / close
        )

        btc_by_open_time = DataFrame(
            {
                "open_time": df_btc["open_time"].astype("int64"),
                "btc_close": df_btc["close"].astype(float),
            }
        ).drop_duplicates("open_time", keep="last")
        benchmark = result[["open_time"]].merge(
            btc_by_open_time, on="open_time", how="left", sort=False
        )["btc_close"]
        result["btc_momentum_3"] = benchmark.pct_change(self.MOMENTUM_BARS).to_numpy()
        btc_ema20 = benchmark.ewm(span=20, adjust=False).mean()
        btc_ema50 = benchmark.ewm(span=50, adjust=False).mean()
        result["btc_trend_score"] = ((btc_ema20 - btc_ema50) / btc_ema50).to_numpy()
        result["relative_strength"] = result["momentum_3"] - result["btc_momentum_3"]
        return result

    def _is_candidate(self, row: Any) -> bool:
        required_values = (
            row["pump_score"],
            row["score_threshold"],
            row["momentum_atr"],
            row["close"],
            row["prior_high"],
            row["close_location"],
            row["relative_volume"],
            row["volume_threshold"],
            row["trend_score"],
            row["ema20"],
            row["relative_strength"],
            row["btc_momentum_3"],
            row["btc_trend_score"],
        )
        if not all(isfinite(float(value)) for value in required_values):
            return False
        return bool(
            row["score_cross"]
            and self.MIN_MOMENTUM_ATR <= row["momentum_atr"] <= self.MAX_MOMENTUM_ATR
            and row["close"] > row["prior_high"]
            and row["close_location"] >= self.MIN_CLOSE_LOCATION
            and row["relative_volume"] >= row["volume_threshold"]
            and row["trend_score"] > 0
            and row["close"] > row["ema20"]
            and row["relative_strength"] > 0
            and row["btc_momentum_3"] > 0
            and row["btc_trend_score"] > 0
        )

    async def signal(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        """Collect a qualified candidate for cross-symbol portfolio selection."""
        df = self.ti.df_15m
        df_btc = self.ti.df_btc_15m
        if (
            self.market_type != MarketType.FUTURES
            or df is None
            or df.empty
            or df_btc is None
            or df_btc.empty
            or len(df) < self.SCORE_LOOKBACK + self.COMPRESSION_BARS + 2
            or "open_time" not in df
            or "open_time" not in df_btc
        ):
            return

        algo = self.ALGO
        base_asset = self.current_symbol_data.base_asset
        df = self.compute_pump_score(df, df_btc)
        row = df.iloc[-2]
        candle_open_time = int(row["open_time"])
        if self.portfolio_selector is not None:
            await self.portfolio_selector.observe(candle_open_time)

        if not self._is_candidate(row):
            return
        score_threshold = float(row["score_threshold"])
        if score_threshold <= 0:
            return
        base_rank_score = float(row["pump_score"]) / score_threshold
        btc_momentum = float(row["btc_momentum_3"])

        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        should_enter, route_reason = self.long_entry_routing(
            context=context,
            symbol_features=symbol_features,
            btc_momentum=float(btc_momentum),
        )
        if not should_enter:
            return

        positioning_evidence = self._positioning_evidence(symbol_features)
        rank_score = base_rank_score * float(
            positioning_evidence["positioning_rank_multiplier"]
        )

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )
        bot_params = BotBase(
            pair=self.symbol,
            name=algo,
            position=Position.long,
            market_type=self.market_type,
            cooldown=self.ENTRY_COOLDOWN_MINUTES,
            dynamic_trailing=False,
            stop_loss=self.LONG_STOP_LOSS_PCT,
            take_profit=self.LONG_TAKE_PROFIT_PCT,
            trailing=False,
            margin_short_reversal=False,
        )

        value = SignalsConsumer(
            direction="LONG",
            autotrade=True,
            bot_params=bot_params,
            score=rank_score,
            current_price=current_price,
            volume=float(row.volume),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        market_breadth = self._latest_market_breadth(context) if context else None
        market_breadth_recovery = (
            self._market_breadth_recovery(context) if context else None
        )
        positioning = symbol_features.derivatives if symbol_features else None
        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: long
            - Rule intent: BUY the strongest price-qualified liquidation-style impulse; derivatives evidence promotes candidates but never blocks them
            - Portfolio rank score (base / evidence-adjusted): {base_rank_score:.2f} / {rank_score:.2f}
            - Positioning evidence / rank multiplier: {float(positioning_evidence["positioning_evidence_score"]):.2f} / {float(positioning_evidence["positioning_rank_multiplier"]):.3f}
            - Pump score / threshold: {float(row["pump_score"]):.4f} / {score_threshold:.4f}
            - Volume: {round_numbers(float(row.volume), decimals=self.price_precision)} {base_asset}
            - OI change (15m): {round_numbers(positioning.oi_change_15m, 5) if positioning and positioning.oi_change_15m is not None else "UNAVAILABLE"}
            - Long / short liquidations: {round_numbers(positioning.long_liquidation_notional, 2) if positioning else "UNAVAILABLE"} / {round_numbers(positioning.short_liquidation_notional, 2) if positioning else "UNAVAILABLE"}
            - Liquidation intensity: {round_numbers(positioning.liquidation_intensity, 5) if positioning and positioning.liquidation_intensity is not None else "UNAVAILABLE"}
            - Market breadth: {round_numbers(market_breadth, 3) if market_breadth is not None else "UNAVAILABLE"}
            - Market breadth recovery: {round_numbers(market_breadth_recovery, 3) if market_breadth_recovery is not None else "UNAVAILABLE"}
            - BTC momentum: {round_numbers(float(btc_momentum), 5)}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Autotrade route: {route_reason}
            - Market stress: {round_numbers(context.market_stress_score, 3) if context else 0}
            - Trigger candle: {candle_open_time}
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Exit profile: fixed {self.LONG_STOP_LOSS_PCT}% stop / {self.LONG_TAKE_PROFIT_PCT}% take profit / 8-candle maximum hold
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        async def dispatch_winner() -> None:
            self.ti.finalize_signal_bot_params(value)
            await self.ti.dispatch_signal_record(
                value=value,
                indicators={
                    "candidate_open_time": candle_open_time,
                    "base_portfolio_rank_score": base_rank_score,
                    "portfolio_rank_score": rank_score,
                    "pump_score": float(row["pump_score"]),
                    "score_threshold": score_threshold,
                    "market_breadth": market_breadth,
                    "market_breadth_recovery": market_breadth_recovery,
                    "momentum_atr": float(row["momentum_atr"]),
                    "relative_volume": float(row["relative_volume"]),
                    "relative_strength": float(row["relative_strength"]),
                    **positioning_evidence,
                    "derivatives_positioning": (
                        positioning.model_dump(mode="json") if positioning else None
                    ),
                    "max_positioning_rank_bonus": self.MAX_POSITIONING_RANK_BONUS,
                },
            )
            self.telegram_consumer.dispatch_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        candidate = LiquidationSweepCandidate(
            candle_open_time=candle_open_time,
            symbol=self.symbol,
            rank_score=rank_score,
            dispatch=dispatch_winner,
        )
        if self.portfolio_selector is None:
            await dispatch_winner()
            return
        await self.portfolio_selector.submit(candidate)

import logging
from datetime import UTC, datetime
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING, Any

from pandas import DataFrame
from pybinbot import (
    BotBase,
    Candles,
    HABollinguerSpread,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
    timestamp_sort_key,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import is_regime_stable, resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ConservativeSpikeHunter:
    """Emit only high-confidence, context-aligned futures spike telemetry."""

    ALGO = "conservative_spike_hunter"
    MIN_CANDLES = 60
    CANDLE_INTERVAL_MS = 15 * 60 * 1000

    VOLUME_RATIO_MIN = 1.8
    VOLUME_MA_WINDOW = 12
    VOLUME_CLUSTER_WINDOW = 8
    VOLUME_CLUSTER_MIN_COUNT = 3
    PRICE_BREAK_FLOOR = 0.035
    PRICE_BREAK_WINDOW = 60
    PRICE_BREAK_QUANTILE = 0.90
    CUMULATIVE_WINDOW = 3
    CUMULATIVE_MOVE_MIN = 0.03
    VOLUME_ACCEL_WINDOW = 3
    VOLUME_ACCEL_MIN = 0.55
    ACCEL_PRICE_MOVE_MIN = 0.02
    STREAK_LENGTH = 3
    BODY_SIZE_MIN = 0.01

    MIN_CONTEXT_CONFIDENCE = 0.90
    MIN_CONTEXT_COVERAGE = 0.90
    MAX_MARKET_STRESS = 0.20
    MIN_BREADTH_MOMENTUM_POINTS = 1.0
    MIN_DIRECTIONAL_REGIME_SCORE = 0.75
    MIN_DIRECTIONAL_TAILWIND = 0.30
    MIN_BTC_ALIGNMENT = 0.10
    MIN_BREADTH_PARTICIPATION = 0.60
    MIN_MICRO_REGIME_STRENGTH = 0.80
    MIN_SYMBOL_TREND_SCORE = 0.02
    MIN_RELATIVE_STRENGTH_VS_BTC = 0.01
    EMIT_THRESHOLD = 0.95

    COMPONENT_WEIGHTS = {
        "volume_cluster": 0.10,
        "price_break": 0.10,
        "cumulative_move": 0.10,
        "acceleration": 0.10,
        "candle_streak": 0.10,
        "breadth_momentum": 0.06,
        "regime_score": 0.06,
        "tailwind": 0.06,
        "btc_alignment": 0.06,
        "breadth_participation": 0.06,
        "micro_regime_strength": 0.05,
        "ema20_alignment": 0.05,
        "ema50_alignment": 0.05,
        "symbol_leadership": 0.05,
    }

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.telegram_consumer = cls.telegram_consumer
        self.market_breadth_data = cls.market_breadth_data
        self.strategy_cooldowns = cls.strategy_cooldowns
        self._last_emitted_candle: int | None = None

    @staticmethod
    def _finite_float(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if isfinite(parsed) else None

    def _breadth_momentum_points(self) -> float | None:
        values = self.market_breadth_data["market_breadth_ma"]
        timestamps = self.market_breadth_data["timestamp"]
        if not isinstance(values, list) or not isinstance(timestamps, list):
            return None
        if len(values) < 2 or len(timestamps) < len(values):
            return None

        timestamped_values: list[tuple[float, float]] = []
        for timestamp, value in zip(timestamps, values, strict=False):
            sort_key = timestamp_sort_key(timestamp)
            breadth_value = self._finite_float(value)
            if sort_key is not None and breadth_value is not None:
                timestamped_values.append((sort_key, breadth_value))

        if len(timestamped_values) < 2:
            return None

        ordered = sorted(timestamped_values, key=lambda item: item[0])
        return (ordered[-1][1] - ordered[-2][1]) * 100

    def _validated_closed_history(self, now_ms: int) -> DataFrame | None:
        df = self.ti.df_15m.copy()
        required_columns = {
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
        }
        if not required_columns.issubset(df.columns):
            return None

        completed, _ = Candles.partition_closed_candles(
            df.to_dict(orient="records"),
            now_ms=now_ms,
            interval_ms=self.CANDLE_INTERVAL_MS,
        )
        completed_open_times = {candle["open_time"] for candle in completed}
        closed = (
            df[df["open_time"].isin(completed_open_times)]
            .sort_values("open_time")
            .copy()
        )
        if len(closed) < self.MIN_CANDLES:
            return None
        if closed[list(required_columns)].tail(self.MIN_CANDLES).isnull().any().any():
            return None
        return closed

    def _resolve_direction(
        self,
        context: LiveMarketContext,
        symbol_features: SymbolMarketFeatures,
    ) -> Position | None:
        if (
            context.market_regime == "TREND_UP"
            and symbol_features.micro_regime == "TREND_UP"
        ):
            return Position.long
        if (
            context.market_regime == "TREND_DOWN"
            and symbol_features.micro_regime == "TREND_DOWN"
        ):
            return Position.short
        return None

    def _passes_context_vetoes(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
        candidate_open_time: int,
    ) -> bool:
        if context is None or symbol_features is None:
            return False
        if (
            not context.is_full
            or context.confidence < self.MIN_CONTEXT_CONFIDENCE
            or context.coverage_ratio < self.MIN_CONTEXT_COVERAGE
            or not context.btc_present
        ):
            return False
        if context.regime_is_transitioning or not is_regime_stable(context):
            return False
        if context.market_stress_score >= self.MAX_MARKET_STRESS:
            return False
        return abs(context.timestamp - candidate_open_time) <= self.CANDLE_INTERVAL_MS

    def _local_measurements(
        self,
        df: DataFrame,
        direction: Position,
    ) -> tuple[dict[str, bool], dict[str, float]]:
        sign = 1.0 if direction == Position.long else -1.0
        opens = df["open"].astype(float)
        closes = df["close"].astype(float)
        volumes = df["volume"].astype(float)
        price_change = closes.pct_change()
        price_change_abs = price_change.abs()
        volume_ma = volumes.rolling(self.VOLUME_MA_WINDOW).mean()
        volume_ratio = volumes / volume_ma

        current_price_change = float(price_change.iloc[-1])
        current_volume_ratio = float(volume_ratio.iloc[-1])
        dynamic_price_threshold = float(
            price_change_abs.rolling(
                self.PRICE_BREAK_WINDOW,
                min_periods=20,
            )
            .quantile(self.PRICE_BREAK_QUANTILE)
            .iloc[-1]
        )
        price_break_threshold = max(
            self.PRICE_BREAK_FLOOR,
            dynamic_price_threshold,
        )
        cumulative_move = float(
            (price_change * sign).clip(lower=0).tail(self.CUMULATIVE_WINDOW).sum()
        )
        volume_acceleration = float(
            current_volume_ratio - volume_ratio.iloc[-1 - self.VOLUME_ACCEL_WINDOW]
        )
        body_size = float(abs(closes.iloc[-1] - opens.iloc[-1]) / opens.iloc[-1])
        candle_directions = (
            closes.tail(self.STREAK_LENGTH) - opens.tail(self.STREAK_LENGTH)
        ) * sign

        components = {
            "volume_cluster": bool(
                current_volume_ratio >= self.VOLUME_RATIO_MIN
                and (
                    volume_ratio.tail(self.VOLUME_CLUSTER_WINDOW)
                    >= self.VOLUME_RATIO_MIN
                ).sum()
                >= self.VOLUME_CLUSTER_MIN_COUNT
            ),
            "price_break": bool(current_price_change * sign >= price_break_threshold),
            "cumulative_move": cumulative_move >= self.CUMULATIVE_MOVE_MIN,
            "acceleration": bool(
                volume_acceleration >= self.VOLUME_ACCEL_MIN
                and current_price_change * sign >= self.ACCEL_PRICE_MOVE_MIN
            ),
            "candle_streak": bool(
                (candle_directions > 0).all() and body_size >= self.BODY_SIZE_MIN
            ),
        }
        measurements = {
            "current_price_change": current_price_change,
            "current_volume_ratio": current_volume_ratio,
            "price_break_threshold": price_break_threshold,
            "cumulative_move": cumulative_move,
            "volume_acceleration": volume_acceleration,
            "body_size": body_size,
        }
        return components, measurements

    def _context_components(
        self,
        context: LiveMarketContext,
        symbol_features: SymbolMarketFeatures,
        direction: Position,
        breadth_momentum_points: float,
    ) -> tuple[dict[str, bool], dict[str, float]]:
        sign = 1.0 if direction == Position.long else -1.0
        directional_regime_score = (
            context.long_regime_score
            if direction == Position.long
            else context.short_regime_score
        )
        directional_tailwind = (
            context.long_tailwind
            if direction == Position.long
            else context.short_tailwind
        )
        breadth_participation = (
            context.advancers_ratio
            if direction == Position.long
            else context.decliners_ratio
        )
        ema20_aligned = (
            symbol_features.above_ema20
            if direction == Position.long
            else not symbol_features.above_ema20
        )
        ema50_aligned = (
            symbol_features.above_ema50
            if direction == Position.long
            else not symbol_features.above_ema50
        )

        components = {
            "breadth_momentum": (
                breadth_momentum_points * sign >= self.MIN_BREADTH_MOMENTUM_POINTS
            ),
            "regime_score": (
                directional_regime_score >= self.MIN_DIRECTIONAL_REGIME_SCORE
            ),
            "tailwind": directional_tailwind >= self.MIN_DIRECTIONAL_TAILWIND,
            "btc_alignment": (
                context.btc_regime_score * sign >= self.MIN_BTC_ALIGNMENT
            ),
            "breadth_participation": (
                breadth_participation >= self.MIN_BREADTH_PARTICIPATION
            ),
            "micro_regime_strength": (
                symbol_features.micro_regime_strength >= self.MIN_MICRO_REGIME_STRENGTH
            ),
            "ema20_alignment": ema20_aligned,
            "ema50_alignment": ema50_aligned,
            "symbol_leadership": bool(
                symbol_features.trend_score * sign >= self.MIN_SYMBOL_TREND_SCORE
                and symbol_features.relative_strength_vs_btc * sign
                >= self.MIN_RELATIVE_STRENGTH_VS_BTC
            ),
        }
        measurements = {
            "breadth_momentum_points": breadth_momentum_points,
            "directional_regime_score": directional_regime_score,
            "directional_tailwind": directional_tailwind,
            "btc_regime_score": context.btc_regime_score,
            "breadth_participation": breadth_participation,
            "micro_regime_strength": symbol_features.micro_regime_strength,
            "symbol_trend_score": symbol_features.trend_score,
            "relative_strength_vs_btc": symbol_features.relative_strength_vs_btc,
        }
        return components, measurements

    @classmethod
    def _confidence_score(cls, components: dict[str, bool]) -> float:
        return round(
            sum(
                cls.COMPONENT_WEIGHTS[name]
                for name, confirmed in components.items()
                if confirmed
            ),
            10,
        )

    def _already_emitted(self, candle_open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_candle == candle_open_time
        return self.strategy_cooldowns.get((self.ALGO, self.symbol)) == candle_open_time

    def _mark_emitted(self, candle_open_time: int) -> None:
        self._last_emitted_candle = candle_open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = candle_open_time

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            logging.info("%s skipped: market_type_not_futures", self.ALGO)
            return
        if not self.market_breadth_data or not {
            "market_breadth_ma",
            "timestamp",
        }.issubset(self.market_breadth_data):
            logging.info("%s skipped: market_breadth_unavailable", self.ALGO)
            return

        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        df = self._validated_closed_history(now_ms)
        if df is None:
            logging.info("%s skipped: incomplete_candle_history", self.ALGO)
            return

        candidate = df.iloc[-1]
        candidate_open_time = int(candidate["open_time"])
        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        if not self._passes_context_vetoes(
            context=context,
            symbol_features=symbol_features,
            candidate_open_time=candidate_open_time,
        ):
            logging.info("%s skipped: context_veto", self.ALGO)
            return
        assert context is not None
        assert symbol_features is not None

        direction = self._resolve_direction(context, symbol_features)
        if direction is None:
            logging.info("%s skipped: direction_not_aligned", self.ALGO)
            return

        breadth_momentum_points = self._breadth_momentum_points()
        if breadth_momentum_points is None:
            logging.info("%s skipped: breadth_momentum_unavailable", self.ALGO)
            return

        local_components, local_measurements = self._local_measurements(df, direction)
        context_components, context_measurements = self._context_components(
            context=context,
            symbol_features=symbol_features,
            direction=direction,
            breadth_momentum_points=breadth_momentum_points,
        )
        components = local_components | context_components
        confidence_score = self._confidence_score(components)
        if confidence_score < self.EMIT_THRESHOLD:
            logging.info(
                "%s skipped: confidence %.2f below %.2f",
                self.ALGO,
                confidence_score,
                self.EMIT_THRESHOLD,
            )
            return
        if self._already_emitted(candidate_open_time):
            logging.info("%s skipped: candle_already_emitted", self.ALGO)
            return

        self._mark_emitted(candidate_open_time)
        direction_label = direction.value.upper()
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            MarketType.FUTURES,
            self.symbol,
        )
        measurements = local_measurements | context_measurements
        indicators = {
            "confidence_threshold": self.EMIT_THRESHOLD,
            "candidate_open_time": candidate_open_time,
            "context_timestamp": context.timestamp,
            "route": f"strict_{direction.value}_context_alignment",
            "components": components,
            "component_weights": self.COMPONENT_WEIGHTS,
            "measurements": measurements,
        }
        value = SignalsConsumer(
            direction=direction_label,
            autotrade=False,
            current_price=current_price,
            volume=float(candidate["volume"]),
            score=confidence_score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=direction,
                market_type=MarketType.FUTURES,
                margin_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        base_asset = self.current_symbol_data.base_asset
        quote_asset = self.current_symbol_data.quote_asset
        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {direction_label} SHADOW ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Candidate candle: {datetime.fromtimestamp(candidate_open_time / 1000, tz=UTC)}
            - Confidence score: {confidence_score:.2f} / {self.EMIT_THRESHOLD:.2f}
            - Volume: {round_numbers(float(candidate["volume"]), decimals=self.price_precision)} {base_asset}
            - Quote volume: {round_numbers(float(candidate["quote_asset_volume"]), decimals=self.price_precision)} {quote_asset}
            - Market regime: {context.market_regime}
            - Coin regime: {symbol_features.micro_regime}
            - Breadth momentum: {breadth_momentum_points:.2f} points
            {format_context_timestamp_line(context)}
            - Route: strict_{direction.value}_context_alignment
            - Shadow mode: autotrade is disabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)

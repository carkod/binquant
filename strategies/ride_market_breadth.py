import logging
from datetime import UTC, datetime
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING

from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketBreadthSeries,
    MarketType,
    Position,
    SignalsConsumer,
    coerce_number,
    round_numbers,
    timestamp_sort_key,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class RideMarketBreadth:
    ALGO = "ride_market_breadth"

    MAX_MARKET_STRESS_SCORE = 0.35
    MIN_BREADTH_POINTS_PER_BAR = 0.010
    MIN_BREADTH_ACCELERATION = 0.003
    POSITIVE_BREADTH_SHORT_LEVEL = 0.15
    NEGATIVE_BREADTH_LONG_LEVEL = -0.15
    ZERO_CROSS_LOOKAHEAD_BARS = 12

    VOLUME_MA_WINDOW = 20
    MIN_VOLUME_RATIO = 0.9
    MIN_DIRECTIONAL_BODY_PCT = 0.0015

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.strategy_cooldowns = cls.strategy_cooldowns
        self.market_breadth_data: MarketBreadthSeries | None = cls.market_breadth_data
        self._last_emitted_candle: int | None = None

    def _ordered_breadth_series(self) -> list[tuple[list[float], str]]:
        market_breadth_data = self.market_breadth_data
        if market_breadth_data is None:
            return []

        ordered_series: list[tuple[list[float], str]] = []
        for key in ("market_breadth_ma", "market_breadth"):
            values = getattr(market_breadth_data, key)
            timestamped_values: list[tuple[float, float]] = []
            if len(values) >= 3 and len(market_breadth_data.timestamp) >= len(values):
                for timestamp, value in zip(
                    market_breadth_data.timestamp, values, strict=False
                ):
                    sort_key = timestamp_sort_key(timestamp)
                    breadth_value = coerce_number(value)
                    if sort_key is not None and breadth_value is not None:
                        timestamped_values.append((sort_key, breadth_value))

            if len(timestamped_values) >= 3:
                ordered_series.append(
                    (
                        [
                            breadth_value
                            for _, breadth_value in sorted(
                                timestamped_values, key=lambda item: item[0]
                            )
                        ],
                        key,
                    )
                )
                continue

            parsed_values = [
                parsed
                for value in values
                if (parsed := coerce_number(value)) is not None
            ]
            if len(parsed_values) >= 3:
                ordered_series.append((list(reversed(parsed_values)), key))

        return ordered_series

    def _breadth_setup(self) -> tuple[Position | None, str, dict[str, float | str]]:
        best_indicators: dict[str, float | str] = {"breadth_source": "unavailable"}
        for values, source in self._ordered_breadth_series():
            if len(values) < 4:
                continue

            direction, reason, indicators = self._evaluate_breadth_values(
                values=values,
                source=source,
            )
            best_indicators = indicators
            if direction is not None:
                return direction, reason, indicators

        if best_indicators["breadth_source"] == "unavailable":
            return None, "breadth_history_unavailable", best_indicators

        return None, "breadth_no_actionable_break", best_indicators

    def _evaluate_breadth_values(
        self,
        *,
        values: list[float],
        source: str,
    ) -> tuple[Position | None, str, dict[str, float | str]]:
        latest = values[-1]
        previous = values[-2]
        prior = values[-3]
        older = values[-4]
        previous_slope = previous - prior
        current_slope = latest - previous
        prior_slope = prior - older
        acceleration = current_slope - previous_slope

        indicators: dict[str, float | str] = {
            "breadth_source": source,
            "breadth_latest": latest,
            "breadth_previous": previous,
            "breadth_current_slope": current_slope,
            "breadth_previous_slope": previous_slope,
            "breadth_prior_slope": prior_slope,
            "breadth_acceleration": acceleration,
        }

        broke_downtrend_up = (
            previous_slope < 0
            and current_slope >= self.MIN_BREADTH_POINTS_PER_BAR
            and acceleration >= self.MIN_BREADTH_ACCELERATION
        )
        if latest >= self.POSITIVE_BREADTH_SHORT_LEVEL and broke_downtrend_up:
            return Position.short, "positive_breadth_downtrend_break_pop", indicators

        recovering_to_zero = (
            latest < 0
            and current_slope >= self.MIN_BREADTH_POINTS_PER_BAR
            and acceleration >= self.MIN_BREADTH_ACCELERATION
            and latest + (current_slope * self.ZERO_CROSS_LOOKAHEAD_BARS) >= 0
        )
        if latest <= self.NEGATIVE_BREADTH_LONG_LEVEL and recovering_to_zero:
            return Position.long, "negative_breadth_recovery_to_zero", indicators

        return None, "breadth_no_actionable_break", indicators

    def _log_breadth_skip(
        self,
        *,
        reason: str,
        indicators: dict[str, float | str],
    ) -> None:
        logging.info(
            "%s skipped: %s source=%s latest=%s previous_slope=%s "
            "current_slope=%s acceleration=%s projected_zero_cross=%s",
            self.ALGO,
            reason,
            indicators.get("breadth_source"),
            round_numbers(float(indicators.get("breadth_latest", 0.0)), 4),
            round_numbers(float(indicators.get("breadth_previous_slope", 0.0)), 4),
            round_numbers(float(indicators.get("breadth_current_slope", 0.0)), 4),
            round_numbers(float(indicators.get("breadth_acceleration", 0.0)), 4),
            round_numbers(
                float(indicators.get("breadth_latest", 0.0))
                + float(indicators.get("breadth_current_slope", 0.0))
                * self.ZERO_CROSS_LOOKAHEAD_BARS,
                4,
            ),
        )

    @staticmethod
    def _micro_regime_confirms(
        direction: Position,
        features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if features is None:
            return False, "symbol_regime_unavailable"

        if direction == Position.long:
            if features.micro_regime in {"TREND_UP", "TRANSITIONAL"} and (
                features.micro_regime_transition
                in {"BREAKOUT_UP", "RECOVERY", "ENTERED_TREND_UP"}
                or features.trend_score > 0
                or features.relative_strength_vs_btc > 0
            ):
                return True, "micro_regime_supports_long"
            return False, "micro_regime_not_long"

        if features.micro_regime in {"TREND_DOWN", "VOLATILE", "TRANSITIONAL"} and (
            features.micro_regime_transition
            in {"BREAKDOWN", "ENTERED_TREND_DOWN", "VOLATILITY_EXPANSION"}
            or features.trend_score < 0
            or features.relative_strength_vs_btc < 0
        ):
            return True, "micro_regime_supports_short"
        return False, "micro_regime_not_short"

    @staticmethod
    def _context_confirms(
        direction: Position,
        context: LiveMarketContext,
    ) -> tuple[bool, str]:
        if context.market_stress_score >= RideMarketBreadth.MAX_MARKET_STRESS_SCORE:
            return False, "market_stress_too_high"

        if direction == Position.long:
            if context.short_regime_score >= context.long_regime_score + 0.15:
                return False, "market_context_strongly_short"
            if context.btc_regime_score < -0.25 and context.btc_return < 0:
                return False, "btc_context_strongly_down"
            return True, "market_context_allows_long"

        if context.long_regime_score >= context.short_regime_score + 0.25:
            return False, "market_context_strongly_long"
        if context.btc_regime_score > 0.35 and context.btc_return > 0.01:
            return False, "btc_context_strongly_up"
        return True, "market_context_allows_short"

    def _local_candle_confirms(
        self,
        direction: Position,
    ) -> tuple[bool, str, dict[str, float]]:
        df = self.ti.df_15m
        if df is None or df.empty:
            return False, "candles_unavailable", {}

        candidate = df.iloc[-1]
        close = float(candidate["close"])
        open_ = float(candidate["open"])
        volume = float(candidate["volume"])
        body_pct = (close - open_) / (open_ + 1e-6)
        momentum_3 = float(df["close"].pct_change(3).iloc[-1]) if len(df) >= 4 else 0.0
        volume_ma = float(df["volume"].rolling(self.VOLUME_MA_WINDOW).mean().iloc[-1])
        volume_ratio = volume / (volume_ma + 1e-6)

        indicators = {
            "body_pct": body_pct,
            "momentum_3": momentum_3,
            "volume_ratio": volume_ratio,
            "volume_ma": volume_ma,
        }

        if not all(isfinite(value) for value in indicators.values()):
            return False, "local_indicators_not_ready", indicators

        if volume_ratio < self.MIN_VOLUME_RATIO:
            return False, "volume_ratio_too_low", indicators

        if direction == Position.long:
            if body_pct >= self.MIN_DIRECTIONAL_BODY_PCT or momentum_3 > 0:
                return True, "local_price_confirms_long", indicators
            return False, "local_price_not_long", indicators

        if body_pct <= -self.MIN_DIRECTIONAL_BODY_PCT or momentum_3 < 0:
            return True, "local_price_confirms_short", indicators
        return False, "local_price_not_short", indicators

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

        context = self.ti.latest_market_context
        if context is None:
            logging.info("%s skipped: market_context_unavailable", self.ALGO)
            return

        direction, breadth_reason, breadth_indicators = self._breadth_setup()
        if direction is None:
            self._log_breadth_skip(
                reason=breadth_reason,
                indicators=breadth_indicators,
            )
            return

        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        micro_confirmed, micro_reason = self._micro_regime_confirms(
            direction=direction,
            features=symbol_features,
        )
        if not micro_confirmed:
            logging.info("%s skipped: %s", self.ALGO, micro_reason)
            return

        context_confirmed, context_reason = self._context_confirms(
            direction=direction,
            context=context,
        )
        if not context_confirmed:
            logging.info("%s skipped: %s", self.ALGO, context_reason)
            return

        local_confirmed, local_reason, local_indicators = self._local_candle_confirms(
            direction=direction,
        )
        if not local_confirmed:
            logging.info("%s skipped: %s", self.ALGO, local_reason)
            return

        candidate = self.ti.df_15m.iloc[-1]
        candidate_open_time = int(candidate["open_time"])
        if self._already_emitted(candidate_open_time):
            logging.info("%s skipped: candle_already_emitted", self.ALGO)
            return
        self._mark_emitted(candidate_open_time)

        direction_label = direction.value.upper()
        entry_price = float(current_price)
        score = round_numbers(
            1.0
            + min(abs(float(breadth_indicators["breadth_current_slope"])) * 10, 0.5)
            + min(abs(local_indicators["momentum_3"]) * 10, 0.5),
            4,
        )
        base_asset = self.current_symbol_data.base_asset
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            MarketType.FUTURES,
            self.symbol,
        )
        autotrade = getenv("ENV") == "staging"

        value = SignalsConsumer(
            direction=direction_label,
            autotrade=autotrade,
            current_price=entry_price,
            volume=float(candidate["volume"]),
            score=score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=direction,
                market_type=MarketType.FUTURES,
                dynamic_trailing=True,
                margin_short_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        rule_intent = (
            "SELL when positive breadth breaks upward after a falling breadth trend, but symbol micro regime and local momentum favor downside follow-through"
            if direction == Position.short
            else "BUY when negative breadth accelerates toward a zero cross and symbol micro regime plus local momentum confirm recovery"
        )
        indicators = {
            **breadth_indicators,
            **local_indicators,
            "breadth_reason": breadth_reason,
            "micro_reason": micro_reason,
            "context_reason": context_reason,
            "local_reason": local_reason,
        }

        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {direction_label} ENTRY
            - Current price: {round_numbers(entry_price, decimals=self.price_precision)}
            - Strategy: {direction.value}
            - Rule intent: {rule_intent}
            - Breadth setup: {breadth_reason}
            - Breadth latest/slope/accel: {round_numbers(float(breadth_indicators["breadth_latest"]), 4)} / {round_numbers(float(breadth_indicators["breadth_current_slope"]), 4)} / {round_numbers(float(breadth_indicators["breadth_acceleration"]), 4)}
            - Market regime: {context.market_regime if context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Local momentum 3-bar: {round_numbers(local_indicators["momentum_3"], 5)}
            - Volume: {round_numbers(float(candidate["volume"]), decimals=self.price_precision)} {base_asset} (ratio {round_numbers(local_indicators["volume_ratio"], 2)}, 20-bar avg {round_numbers(local_indicators["volume_ma"], decimals=self.price_precision)} {base_asset})
            - Confidence score: {score}
            - Signal timestamp: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled outside staging"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

from __future__ import annotations

import logging
from datetime import UTC, datetime
from math import isfinite
from os import getenv
from statistics import median
from typing import TYPE_CHECKING, Any

from pybinbot import (
    BotBase,
    BotModel,
    DealType,
    HABollinguerSpread,
    MarketType,
    OrderStatus,
    Position,
    SignalsConsumer,
    Status,
    round_numbers,
    sec_to_ms,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from pandas import DataFrame

    from producers.context_evaluator import ContextEvaluator


class TopGainerMomentumRecovery:
    """Re-enter a failed top-gainer trade after a confirmed momentum recovery."""

    ALGO = "top_gainer_momentum_recovery"
    SOURCE_ALGO = "top_gainer_early_momentum"
    SOURCE_LOG_PREFIX = "top_gainer_recovery_source_bot_id="

    WATCH_WINDOW_MS = 12 * 60 * 60 * 1000
    SOURCE_HIGH_LOOKBACK_BARS = 48
    BOTTOM_LOOKBACK_BARS = 8
    REACCELERATION_WINDOW_BARS = 3
    VOLUME_CONFIRMATION_BARS = 4

    MIN_DRAWDOWN = 0.03
    MAX_DRAWDOWN = 0.20
    MIN_DRAWDOWN_ATR = 1.0
    BOTTOM_LOW_ATR_TOLERANCE = 0.25
    MIN_BOTTOM_BODY_RETURN = 0.005
    MIN_BOTTOM_CLOSE_LOCATION = 0.75
    MIN_REACCELERATION_RETURN = 0.005
    MIN_REACCELERATION_CLOSE_LOCATION = 0.60
    MIN_RELATIVE_STRENGTH_1H = 0.0
    MIN_RELATIVE_STRENGTH_VS_BTC = 0.03
    MAX_MARKET_STRESS_SCORE = 0.25
    MAX_ENTRY_DISCOUNT = 0.005
    MAX_ENTRY_EXTENSION = 0.015
    MAX_DERIVATIVES_STRESS = 0.80
    ADVERSE_OI_EXPANSION_15M = 0.10

    FIAT_ORDER_SIZE_FRACTION = 1 / 6
    STOP_LOSS_PCT = 2.0
    TRAILING_PROFIT_PCT = 2.0
    TRAILING_DEVIATION_PCT = 0.75
    ENTRY_COOLDOWN_MINUTES = 60
    TERMINAL_ORDER_STATUSES = frozenset(
        {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    )

    def __init__(self, cls: ContextEvaluator) -> None:
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.recent_bots = cls.top_gainer_recovery_bots
        self.attempted_source_ids = cls.top_gainer_recovery_attempted_source_ids

    @classmethod
    def source_log_marker(cls, source_bot_id: str) -> str:
        return f"{cls.SOURCE_LOG_PREFIX}{source_bot_id}"

    @classmethod
    def _source_id_from_log(cls, log: object) -> str | None:
        if not isinstance(log, str) or cls.SOURCE_LOG_PREFIX not in log:
            return None
        source_id = log.split(cls.SOURCE_LOG_PREFIX, 1)[1].strip()
        return source_id or None

    @classmethod
    def consumed_source_ids(cls, bots: list[BotModel]) -> set[str]:
        consumed: set[str] = set()
        for bot in bots:
            if bot.name != cls.ALGO:
                continue
            for log in bot.logs:
                source_id = cls._source_id_from_log(log)
                if source_id is not None:
                    consumed.add(source_id)
        return consumed

    @staticmethod
    def _is_filled_stop_loss(order: Any) -> bool:
        return (
            order.deal_type == DealType.stop_loss and order.status == OrderStatus.FILLED
        )

    @classmethod
    def _eligible_source(cls, bot: BotModel) -> bool:
        if bot.name != cls.SOURCE_ALGO:
            return False

        if bot.status == Status.completed:
            return any(cls._is_filled_stop_loss(order) for order in bot.orders)
        if bot.status not in {Status.error, Status.inactive}:
            return False

        if any(
            order.deal_type == DealType.base_order
            and order.status not in cls.TERMINAL_ORDER_STATUSES
            for order in bot.orders
        ):
            return False

        # An error/inactive database status is not enough to prove that an
        # exchange position was closed. Do not hand it to a new strategy when
        # the deal still describes open exposure.
        return not (bot.deal.opening_qty > 0 and bot.deal.closing_timestamp <= 0)

    @classmethod
    def select_source(
        cls,
        *,
        bots: list[BotModel],
        symbol: str,
        attempted_source_ids: set[str],
        now_ms: int,
    ) -> BotModel | None:
        consumed_source_ids = cls.consumed_source_ids(bots) | attempted_source_ids
        candidates = [
            bot
            for bot in bots
            if bot.pair == symbol
            and cls._eligible_source(bot)
            and str(bot.id) not in consumed_source_ids
            and 0 <= now_ms - sec_to_ms(int(bot.created_at)) <= cls.WATCH_WINDOW_MS
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda bot: sec_to_ms(int(bot.created_at)))

    @staticmethod
    def _completed_candles(df: DataFrame, now_ms: int) -> DataFrame:
        if "close_time" not in df.columns:
            return df.iloc[0:0]
        return df.loc[df["close_time"] < now_ms]

    @staticmethod
    def _source_timestamp(source: BotModel) -> int:
        return sec_to_ms(int(source.created_at))

    @staticmethod
    def _eligible_since(source: BotModel) -> int:
        if source.status == Status.completed:
            return source.deal.closing_timestamp
        return sec_to_ms(int(source.created_at))

    @staticmethod
    def _btc_return_1h(btc_df: DataFrame, open_time: int) -> float | None:
        if len(btc_df) < 5 or not {"open_time", "close"}.issubset(btc_df.columns):
            return None
        matches = btc_df.index[btc_df["open_time"] == open_time].tolist()
        if not matches:
            return None
        position = btc_df.index.get_loc(matches[-1])
        if not isinstance(position, int) or position < 4:
            return None
        anchor = float(btc_df["close"].iloc[position - 4])
        close = float(btc_df["close"].iloc[position])
        if anchor <= 0 or close <= 0:
            return None
        result = close / anchor - 1
        return result if isfinite(result) else None

    @classmethod
    def recovery_setup(
        cls,
        *,
        df: DataFrame,
        btc_df: DataFrame,
        source: BotModel,
    ) -> tuple[dict[str, float | int | str] | None, str]:
        source_rows = df.index[
            df["close_time"] <= cls._source_timestamp(source)
        ].tolist()
        if not source_rows:
            return None, "source_candle_unavailable"
        source_position = df.index.get_loc(source_rows[-1])
        if not isinstance(source_position, int):
            return None, "source_candle_index_invalid"

        current_position = len(df) - 1
        if current_position <= source_position:
            return None, "no_post_source_candles"
        current = df.iloc[current_position]
        if int(current["close_time"]) <= cls._eligible_since(source):
            return None, "source_not_terminal_before_confirmation"

        shelf_start = max(0, source_position - cls.SOURCE_HIGH_LOOKBACK_BARS)
        breakout_shelf = float(df["high"].iloc[shelf_start:source_position].max())
        if not isfinite(breakout_shelf) or breakout_shelf <= 0:
            return None, "breakout_shelf_unavailable"

        bottom_start = max(
            source_position + 1, current_position - cls.REACCELERATION_WINDOW_BARS
        )
        for bottom_position in range(current_position - 1, bottom_start - 1, -1):
            bottom = df.iloc[bottom_position]
            previous = df.iloc[bottom_position - 1]
            bottom_open = float(bottom["open"])
            bottom_high = float(bottom["high"])
            bottom_low = float(bottom["low"])
            bottom_close = float(bottom["close"])
            bottom_atr = float(bottom["ATR"])
            values = (
                bottom_open,
                bottom_high,
                bottom_low,
                bottom_close,
                bottom_atr,
            )
            if not all(isfinite(value) and value > 0 for value in values):
                continue

            peak_highs = [
                float(value)
                for value in df["high"].iloc[source_position : bottom_position + 1]
            ]
            peak_high = max(peak_highs)
            peak_position = source_position + peak_highs.index(peak_high)
            drawdown = 1 - bottom_low / peak_high
            if not cls.MIN_DRAWDOWN <= drawdown <= cls.MAX_DRAWDOWN:
                continue
            if peak_high - bottom_low < cls.MIN_DRAWDOWN_ATR * bottom_atr:
                continue

            local_low_start = max(
                peak_position + 1,
                bottom_position - cls.BOTTOM_LOOKBACK_BARS + 1,
            )
            if local_low_start > bottom_position:
                continue
            local_low = float(
                df["low"].iloc[local_low_start : bottom_position + 1].min()
            )
            if bottom_low > local_low + cls.BOTTOM_LOW_ATR_TOLERANCE * bottom_atr:
                continue

            bottom_range = bottom_high - bottom_low
            if bottom_range <= 0:
                continue
            bottom_body_return = bottom_close / bottom_open - 1
            bottom_close_location = (bottom_close - bottom_low) / bottom_range
            if bottom_body_return < cls.MIN_BOTTOM_BODY_RETURN:
                continue
            if bottom_close_location < cls.MIN_BOTTOM_CLOSE_LOCATION:
                continue
            if bottom_close <= float(previous["close"]):
                continue

            current_open = float(current["open"])
            current_high = float(current["high"])
            current_low = float(current["low"])
            current_close = float(current["close"])
            current_range = current_high - current_low
            if min(current_open, current_high, current_low, current_close) <= 0:
                return None, "reacceleration_values_invalid"
            if current_range <= 0:
                return None, "reacceleration_range_invalid"

            reacceleration_return = current_close / current_open - 1
            reacceleration_close_location = (
                current_close - current_low
            ) / current_range
            if current_close <= bottom_high:
                continue
            if current_close <= breakout_shelf:
                return None, "breakout_shelf_not_reclaimed"
            if reacceleration_return < cls.MIN_REACCELERATION_RETURN:
                return None, "reacceleration_candle_too_weak"
            if (
                reacceleration_close_location + 1e-9
                < cls.MIN_REACCELERATION_CLOSE_LOCATION
            ):
                return None, "reacceleration_close_not_near_high"

            volume_start = max(0, current_position - cls.VOLUME_CONFIRMATION_BARS)
            prior_volumes = [
                float(value)
                for value in df["volume"].iloc[volume_start:current_position]
            ]
            if not prior_volumes or float(current["volume"]) < median(prior_volumes):
                return None, "reacceleration_volume_too_low"

            symbol_anchor = float(df["close"].iloc[current_position - 4])
            if symbol_anchor <= 0:
                return None, "symbol_return_anchor_invalid"
            symbol_return_1h = current_close / symbol_anchor - 1
            btc_return_1h = cls._btc_return_1h(btc_df, int(current["open_time"]))
            if btc_return_1h is None:
                return None, "btc_return_unavailable"
            relative_strength_1h = symbol_return_1h - btc_return_1h
            if relative_strength_1h <= cls.MIN_RELATIVE_STRENGTH_1H:
                return None, "relative_strength_1h_not_positive"

            return (
                {
                    "source_bot_id": str(source.id),
                    "source_status": str(source.status),
                    "source_created_at": sec_to_ms(int(source.created_at)),
                    "source_signal_id": source.signal_id or 0,
                    "breakout_shelf": breakout_shelf,
                    "post_source_peak": peak_high,
                    "drawdown_pct": drawdown * 100,
                    "bottom_open_time": int(bottom["open_time"]),
                    "bottom_low": bottom_low,
                    "bottom_high": bottom_high,
                    "bottom_close": bottom_close,
                    "bottom_body_return": bottom_body_return,
                    "bottom_close_location": bottom_close_location,
                    "reacceleration_open_time": int(current["open_time"]),
                    "reacceleration_close": current_close,
                    "reacceleration_return": reacceleration_return,
                    "reacceleration_close_location": reacceleration_close_location,
                    "relative_strength_1h": relative_strength_1h,
                    "btc_return_1h": btc_return_1h,
                    "volume": float(current["volume"]),
                    "volume_confirmation_floor": median(prior_volumes),
                },
                "top_gainer_recovery_reaccelerated",
            )

        return None, "bottoming_candle_unavailable"

    @classmethod
    def _risk_profile_allows(
        cls,
        *,
        context: LiveMarketContext | None,
        features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"
        if context.market_stress_score >= cls.MAX_MARKET_STRESS_SCORE:
            return False, "market_stress_too_high"
        if features is None:
            return False, "symbol_regime_unavailable"
        if (
            features.relative_strength_vs_btc_horizon
            <= cls.MIN_RELATIVE_STRENGTH_VS_BTC
        ):
            return False, "relative_strength_vs_btc_not_positive"
        if features.micro_regime_transition in {"BREAKDOWN", "ENTERED_TREND_DOWN"}:
            return False, "symbol_transition_not_long"
        if features.micro_regime == "TREND_DOWN" and features.trend_score < 0:
            return False, "symbol_trend_down"

        positioning = features.derivatives
        if (
            positioning is not None
            and positioning.derivatives_stress_score >= cls.MAX_DERIVATIVES_STRESS
            and positioning.oi_change_15m is not None
            and positioning.oi_change_15m >= cls.ADVERSE_OI_EXPANSION_15M
        ):
            return False, "adverse_derivatives_expansion"
        return True, "risk_profile_allows_recovery_long"

    def _fiat_order_size(self) -> float:
        settings = getattr(self.at_consumer, "autotrade_settings", None)
        base_order_size = float(getattr(settings, "base_order_size", 0.0) or 0.0)
        return round_numbers(base_order_size * self.FIAT_ORDER_SIZE_FRACTION, 8)

    def _symbol_is_free(self) -> tuple[bool, str]:
        active_pairs = self.ti.binbot_api.get_active_pairs(collection_name="bots")
        if self.symbol in active_pairs:
            return False, "active_bot_or_entry_order_exists"
        try:
            position = self.at_consumer.kucoin_futures_api.get_futures_position(
                self.symbol
            )
        except Exception:
            logging.exception(
                "%s skipped: unable to verify exchange position for %s",
                self.ALGO,
                self.symbol,
            )
            return False, "exchange_position_unavailable"
        current_qty = float(getattr(position, "current_qty", 0) or 0)
        if abs(current_qty) > 0:
            return False, "exchange_position_exists"
        return True, "symbol_is_free"

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            return

        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        source = self.select_source(
            bots=self.recent_bots,
            symbol=self.symbol,
            attempted_source_ids=self.attempted_source_ids,
            now_ms=now_ms,
        )
        if source is None:
            return

        df = self._completed_candles(self.ti.df_15m, now_ms)
        btc_df = self._completed_candles(self.ti.df_btc_15m, now_ms)
        required_columns = {
            "open_time",
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ATR",
        }
        if len(df) < self.SOURCE_HIGH_LOOKBACK_BARS + 5:
            logging.info("%s skipped: history_too_short", self.ALGO)
            return
        if not required_columns.issubset(df.columns):
            logging.info("%s skipped: required_columns_unavailable", self.ALGO)
            return

        setup, setup_reason = self.recovery_setup(
            df=df,
            btc_df=btc_df,
            source=source,
        )
        if setup is None:
            logging.info("%s skipped: %s", self.ALGO, setup_reason)
            return

        confirmation_close = float(setup["reacceleration_close"])
        entry_distance = current_price / confirmation_close - 1
        if not -self.MAX_ENTRY_DISCOUNT <= entry_distance <= self.MAX_ENTRY_EXTENSION:
            logging.info("%s skipped: live_price_moved_beyond_entry_window", self.ALGO)
            return

        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        risk_allowed, risk_reason = self._risk_profile_allows(
            context=context,
            features=symbol_features,
        )
        if not risk_allowed:
            logging.info("%s skipped: %s", self.ALGO, risk_reason)
            return

        symbol_is_free, symbol_reason = self._symbol_is_free()
        if not symbol_is_free:
            logging.info("%s skipped: %s", self.ALGO, symbol_reason)
            return

        source_id = str(source.id)
        # Mark before the first await so duplicate kline deliveries cannot race
        # a second recovery signal for the same source bot.
        self.attempted_source_ids.add(source_id)

        fiat_order_size = self._fiat_order_size()
        base_asset = self.current_symbol_data.base_asset
        quote_asset = self.current_symbol_data.quote_asset
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )
        indicators = {
            **setup,
            "entry_reason": setup_reason,
            "risk_reason": risk_reason,
            "entry_distance_pct": entry_distance * 100,
            "stop_loss_pct": self.STOP_LOSS_PCT,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
        }
        value = SignalsConsumer(
            direction=Position.long.value.upper(),
            autotrade=True,
            current_price=float(current_price),
            volume=float(setup["volume"]),
            score=round_numbers(1 + float(setup["relative_strength_1h"]), 4),
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.long,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=False,
                fiat_order_size=fiat_order_size,
                stop_loss=self.STOP_LOSS_PCT,
                trailing=True,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                trailing_profit=self.TRAILING_PROFIT_PCT,
                margin_short_reversal=False,
                logs=[self.source_log_marker(source_id)],
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.ti.finalize_signal_bot_params(value)
        assert value.bot_params is not None

        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG RECOVERY ENTRY
            - Source bot: {source_id} ({source.status})
            - Current price: {round_numbers(float(current_price), self.price_precision)}
            - Pullback: {round_numbers(float(setup["drawdown_pct"]), 2)}% from {round_numbers(float(setup["post_source_peak"]), self.price_precision)}
            - Bottom low / close: {round_numbers(float(setup["bottom_low"]), self.price_precision)} / {round_numbers(float(setup["bottom_close"]), self.price_precision)}
            - Breakout shelf / recovery close: {round_numbers(float(setup["breakout_shelf"]), self.price_precision)} / {round_numbers(confirmation_close, self.price_precision)}
            - 1h relative strength vs BTC: {round_numbers(float(setup["relative_strength_1h"]) * 100, 2)}%
            - Volume: {round_numbers(float(setup["volume"]), self.price_precision)} {base_asset}
            - Market regime: {context.market_regime if context else "UNAVAILABLE"}
            {format_context_timestamp_line(context)}
            - Max margin: {value.bot_params.fiat_order_size} {quote_asset}
            - Stop loss: {self.STOP_LOSS_PCT}%
            - Trailing activation / deviation: {self.TRAILING_PROFIT_PCT}% / {self.TRAILING_DEVIATION_PCT}%
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        await self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

from __future__ import annotations

import logging
from datetime import UTC, datetime
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING, Any

from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
)

from shared.utils import build_links_msg

if TYPE_CHECKING:
    from pandas import DataFrame

    from producers.context_evaluator import ContextEvaluator


class RelativeStrengthImpulseRider:
    """Buy isolated one-hour impulses after a confirmed one-candle retest.

    A setup is armed after the impulse confirmation closes. The immediately
    following completed candle must touch the one-percent discount, reclaim it
    with a bullish close, and avoid invalidating the trigger. Binbot then owns
    prompt entry execution and the eight-candle maximum holding period.
    """

    ALGO = "relative_strength_impulse_rider"
    BTC_SYMBOL = "XBTUSDTM"
    MIN_HISTORY = 20

    MIN_IMPULSE_RETURN_1H = 0.05
    MAX_IMPULSE_RETURN_1H = 0.18
    MIN_RELATIVE_STRENGTH_1H = 0.08
    MAX_BTC_RETURN_1H = 0.01
    MIN_CONFIRMATION_RETURN = 0.0
    MIN_CONFIRMATION_CLOSE_LOCATION = 0.70
    MAX_ATR_PCT = 0.06

    RETEST_DISCOUNT_PCT = 1.0
    RETEST_WAIT_BARS = 1
    MAX_RETEST_INVALIDATION_PCT = 2.0
    STOP_LOSS_PCT = 2.0
    TRAILING_ACTIVATION_PCT = 5.0
    TRAILING_DEVIATION_PCT = 2.0
    TAKE_PROFIT_PCT = 12.0
    MAX_HOLDING_BARS = 8
    ENTRY_COOLDOWN_MINUTES = 240
    FIAT_ORDER_SIZE_FRACTION = 1 / 3

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
        self.strategy_cooldowns = cls.strategy_cooldowns
        self.strategy_states = cls.strategy_states
        self._last_emitted_candle: int | None = None

    @property
    def _state_key(self) -> tuple[str, str]:
        return self.ALGO, self.symbol

    def _state(self) -> dict[str, float | int] | None:
        return self.strategy_states.get(self._state_key)

    def _set_state(self, state: dict[str, float | int]) -> None:
        self.strategy_states[self._state_key] = state

    def _clear_state(self) -> None:
        self.strategy_states.pop(self._state_key, None)

    @staticmethod
    def _completed_candles(df: DataFrame, now_ms: int) -> DataFrame:
        if "close_time" not in df.columns:
            return df.iloc[0:0]
        return df.loc[df["close_time"] < now_ms]

    @staticmethod
    def _btc_return_at(btc_df: DataFrame, open_time: int) -> float | None:
        if len(btc_df) < 5 or not {"open_time", "close"}.issubset(btc_df.columns):
            return None

        position = next(
            (
                index
                for index in range(len(btc_df) - 1, -1, -1)
                if int(btc_df["open_time"].iloc[index]) == open_time
            ),
            None,
        )
        if position is None or position < 4:
            return None

        anchor = float(btc_df["close"].iloc[position - 4])
        close = float(btc_df["close"].iloc[position])
        if anchor <= 0 or close <= 0:
            return None
        btc_return = close / anchor - 1
        return btc_return if isfinite(btc_return) else None

    @classmethod
    def _features(
        cls,
        df: DataFrame,
        btc_df: DataFrame,
    ) -> tuple[dict[str, float | int] | None, str]:
        required = {"open_time", "open", "high", "low", "close", "ATR"}
        if len(df) < cls.MIN_HISTORY:
            return None, "history_too_short"
        if not required.issubset(df.columns):
            return None, "required_columns_unavailable"

        trigger_index = len(df) - 2
        confirmation_index = len(df) - 1
        trigger = df.iloc[trigger_index]
        confirmation = df.iloc[confirmation_index]
        trigger_close = float(trigger["close"])
        trigger_open = float(trigger["open"])
        confirmation_open = float(confirmation["open"])
        confirmation_high = float(confirmation["high"])
        confirmation_low = float(confirmation["low"])
        confirmation_close = float(confirmation["close"])
        atr = float(trigger["ATR"])
        values = (
            trigger_close,
            trigger_open,
            confirmation_open,
            confirmation_high,
            confirmation_low,
            confirmation_close,
            atr,
        )
        if not all(isfinite(value) and value > 0 for value in values):
            return None, "invalid_candle_values"

        impulse_anchor = float(df["close"].iloc[trigger_index - 4])
        previous_anchor = float(df["close"].iloc[trigger_index - 5])
        previous_close = float(df["close"].iloc[trigger_index - 1])
        if impulse_anchor <= 0 or previous_anchor <= 0 or previous_close <= 0:
            return None, "invalid_return_anchors"

        impulse_return = trigger_close / impulse_anchor - 1
        previous_impulse_return = previous_close / previous_anchor - 1
        trigger_open_time = int(trigger["open_time"])
        btc_return = cls._btc_return_at(btc_df, trigger_open_time)
        if btc_return is None:
            return None, "btc_return_unavailable"

        relative_strength = impulse_return - btc_return
        atr_pct = atr / trigger_close
        confirmation_return = confirmation_close / confirmation_open - 1
        confirmation_range = confirmation_high - confirmation_low
        if confirmation_range <= 0:
            return None, "confirmation_range_invalid"
        confirmation_close_location = (
            confirmation_close - confirmation_low
        ) / confirmation_range

        derived = (
            impulse_return,
            previous_impulse_return,
            btc_return,
            relative_strength,
            atr_pct,
            confirmation_return,
            confirmation_close_location,
        )
        if not all(isfinite(value) for value in derived):
            return None, "indicators_not_ready"
        if not (
            cls.MIN_IMPULSE_RETURN_1H <= impulse_return <= cls.MAX_IMPULSE_RETURN_1H
        ):
            return None, "impulse_outside_range"
        if previous_impulse_return >= cls.MIN_IMPULSE_RETURN_1H:
            return None, "impulse_did_not_cross_threshold"
        if btc_return > cls.MAX_BTC_RETURN_1H:
            return None, "btc_return_too_high"
        if relative_strength < cls.MIN_RELATIVE_STRENGTH_1H:
            return None, "relative_strength_too_low"
        if atr_pct > cls.MAX_ATR_PCT:
            return None, "atr_pct_too_high"
        if confirmation_close < trigger_close:
            return None, "confirmation_did_not_hold_trigger_close"
        if confirmation_return < cls.MIN_CONFIRMATION_RETURN:
            return None, "confirmation_return_negative"
        if confirmation_close_location < cls.MIN_CONFIRMATION_CLOSE_LOCATION:
            return None, "confirmation_close_not_near_high"

        return (
            {
                "trigger_open_time": trigger_open_time,
                "confirmation_open_time": int(confirmation["open_time"]),
                "trigger_close": trigger_close,
                "confirmation_close": confirmation_close,
                "impulse_return_1h": impulse_return,
                "previous_impulse_return_1h": previous_impulse_return,
                "btc_return_1h": btc_return,
                "relative_strength_1h": relative_strength,
                "trigger_return_15m": trigger_close / trigger_open - 1,
                "confirmation_return_15m": confirmation_return,
                "confirmation_close_location": confirmation_close_location,
                "atr_pct": atr_pct,
            },
            "relative_strength_impulse_confirmed",
        )

    def _fiat_order_size(self) -> float:
        settings = getattr(self.at_consumer, "autotrade_settings", None)
        base_order_size = float(getattr(settings, "base_order_size", 0.0) or 0.0)
        return round_numbers(base_order_size * self.FIAT_ORDER_SIZE_FRACTION, 8)

    @classmethod
    def _confirmed_retest(
        cls,
        candle: Any,
        state: dict[str, float | int],
    ) -> tuple[dict[str, float | int] | None, str]:
        open_ = float(candle["open"])
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        retest_level = float(state["retest_level"])
        invalidation_level = float(state["invalidation_level"])
        if not all(
            isfinite(value) and value > 0
            for value in (open_, high, low, close, retest_level, invalidation_level)
        ):
            return None, "retest_values_invalid"
        if low < invalidation_level:
            return None, "retest_invalidated_trigger"
        if low > retest_level:
            return None, "retest_level_not_touched"
        if close < retest_level:
            return None, "retest_level_not_reclaimed"
        if close <= open_:
            return None, "retest_candle_not_bullish"

        candle_range = high - low
        close_location = (close - low) / candle_range if candle_range > 0 else 1.0
        return (
            {
                **state,
                "retest_open_time": int(candle["open_time"]),
                "retest_open": open_,
                "retest_low": low,
                "retest_close": close,
                "retest_close_location": close_location,
            },
            "relative_strength_retest_reclaimed",
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
        if self.market_type != MarketType.FUTURES or self.symbol == self.BTC_SYMBOL:
            return

        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        df = self._completed_candles(self.ti.df_15m, now_ms)
        btc_df = self._completed_candles(self.ti.df_btc_15m, now_ms)
        state = self._state()
        if state is not None:
            if df.empty:
                return
            candle = df.iloc[-1]
            candle_open_time = int(candle["open_time"])
            expected_retest_open_time = int(state["expected_retest_open_time"])
            if candle_open_time < expected_retest_open_time:
                return
            self._clear_state()
            if candle_open_time > expected_retest_open_time:
                logging.info("%s skipped: retest_window_expired", self.ALGO)
                return
            features, reason = self._confirmed_retest(candle, state)
            if features is None:
                logging.info("%s skipped: %s", self.ALGO, reason)
                return
        else:
            features, reason = self._features(df, btc_df)
            if features is None:
                logging.info("%s skipped: %s", self.ALGO, reason)
                return
            trigger_open_time = int(features["trigger_open_time"])
            confirmation_open_time = int(features["confirmation_open_time"])
            interval_ms = confirmation_open_time - trigger_open_time
            if interval_ms <= 0:
                logging.info("%s skipped: candle_interval_invalid", self.ALGO)
                return
            confirmation_close = float(features["confirmation_close"])
            trigger_close = float(features["trigger_close"])
            self._set_state(
                {
                    **features,
                    "retest_level": confirmation_close
                    * (1 - self.RETEST_DISCOUNT_PCT / 100),
                    "invalidation_level": trigger_close
                    * (1 - self.MAX_RETEST_INVALIDATION_PCT / 100),
                    "expected_retest_open_time": confirmation_open_time
                    + interval_ms * self.RETEST_WAIT_BARS,
                }
            )
            logging.info("%s armed: awaiting_confirmed_retest", self.ALGO)
            return

        retest_open_time = int(features["retest_open_time"])
        if self._already_emitted(retest_open_time):
            logging.info("%s skipped: retest_already_emitted", self.ALGO)
            return
        self._mark_emitted(retest_open_time)

        autotrade = getenv("ENV") == "staging"
        route_reason = (
            "staging_relative_strength_retest" if autotrade else "staging_only_shadow"
        )
        fiat_order_size = self._fiat_order_size()
        score = round_numbers(1 + float(features["relative_strength_1h"]), 4)
        quote_asset = self.current_symbol_data.quote_asset
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        indicators = {
            **features,
            "entry_reason": reason,
            "route_reason": route_reason,
            "retest_discount_pct": self.RETEST_DISCOUNT_PCT,
            "retest_wait_bars": self.RETEST_WAIT_BARS,
            "max_retest_invalidation_pct": self.MAX_RETEST_INVALIDATION_PCT,
            "stop_loss_pct": self.STOP_LOSS_PCT,
            "trailing_activation_pct": self.TRAILING_ACTIVATION_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
            "take_profit_pct": self.TAKE_PROFIT_PCT,
            "max_holding_bars": self.MAX_HOLDING_BARS,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
        }

        value = SignalsConsumer(
            direction=Position.long.value.upper(),
            autotrade=autotrade,
            current_price=float(current_price),
            score=score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.long,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=False,
                fiat_order_size=fiat_order_size,
                stop_loss=self.STOP_LOSS_PCT,
                take_profit=self.TAKE_PROFIT_PCT,
                trailing=True,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                trailing_profit=self.TRAILING_ACTIVATION_PCT,
                margin_short_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.ti.finalize_signal_bot_params(value)
        assert value.bot_params is not None
        fiat_order_size = value.bot_params.fiat_order_size

        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG CONFIRMED RETEST ENTRY
            - Confirmation close: {round_numbers(float(features["confirmation_close"]), self.price_precision)}
            - Retest level / low / reclaim close: {round_numbers(float(features["retest_level"]), self.price_precision)} / {round_numbers(float(features["retest_low"]), self.price_precision)} / {round_numbers(float(features["retest_close"]), self.price_precision)}
            - 1h impulse / BTC / relative strength: {round_numbers(float(features["impulse_return_1h"]) * 100, 2)}% / {round_numbers(float(features["btc_return_1h"]) * 100, 2)}% / {round_numbers(float(features["relative_strength_1h"]) * 100, 2)}%
            - Confirmation return / close location: {round_numbers(float(features["confirmation_return_15m"]) * 100, 2)}% / {round_numbers(float(features["confirmation_close_location"]) * 100, 2)}%
            - ATR: {round_numbers(float(features["atr_pct"]) * 100, 2)}%
            - Retest confirmation window: {self.RETEST_WAIT_BARS} candle
            - Stop / trailing activation / deviation / target: {self.STOP_LOSS_PCT}% / {self.TRAILING_ACTIVATION_PCT}% / {self.TRAILING_DEVIATION_PCT}% / {self.TAKE_PROFIT_PCT}%
            - Maximum holding: {self.MAX_HOLDING_BARS} candles after fill
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Max margin: {fiat_order_size} {quote_asset}
            - Autotrade route: {route_reason}
            - Confidence score: {score}
            - Signal timestamp: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled outside staging"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

from datetime import UTC, datetime
from time import time
from typing import TYPE_CHECKING

from pandas import DataFrame, to_numeric

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class HigherLowPattern:
    """
    Detects a higher-low price-structure pattern on 15m candles: after a
    downtrend forms a swing low, price bounces and dips again but the new
    swing low fails to break below the prior one. This is a classic early
    signal that downward momentum is fading, often preceding a reversal or
    a consolidation range.

    Exact sign-mirror of LowerHighPattern (strategies/lower_high_pattern.py):
    swing highs become swing lows, "rise into the peak" becomes "drop into
    the trough", and "drop below the first peak" becomes "rise above the
    first trough".

    Notification only: this strategy never builds a SignalsConsumer/BotBase
    payload and never reaches AutotradeConsumer, so no bot can be opened
    from it.

    Swing lows are found with a simple fractal rule: a candle's low must
    strictly fall below the lows of FRACTAL_WING candles on each side of it.
    Only the two most recent confirmed fractal lows in the lookback window
    are compared.
    """

    ALGO = "higher_low_pattern"

    FRACTAL_WING = 2
    LOOKBACK_BARS = 40
    # Minimum drop (%) from the swing high into the first trough: confirms
    # the first trough actually capped a genuine downtrend leg, not just
    # noise.
    MIN_DROP_PCT = 2.0
    # Minimum rise (%) of the second trough above the first: filters out
    # troughs that are equal within noise (which would just be a double
    # bottom).
    MIN_RISE_PCT = 0.5
    ALERT_COOLDOWN_MINUTES = 240

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.symbol = cls.symbol
        self.config = cls.config
        self.telegram_consumer = cls.telegram_consumer
        self.price_precision = cls.price_precision
        self.strategy_cooldowns = cls.strategy_cooldowns
        self._last_emitted_open_time: int | None = None
        self._last_emitted_at: int | None = None

    @classmethod
    def _fractal_lows(cls, window: DataFrame) -> list[int]:
        wing = cls.FRACTAL_WING
        low = window["low"]
        positions = []
        for i in range(wing, len(low) - wing):
            neighborhood = low.iloc[i - wing : i + wing + 1]
            if (
                low.iloc[i] == neighborhood.min()
                and (neighborhood == neighborhood.min()).sum() == 1
            ):
                positions.append(i)
        return positions

    @classmethod
    def _fractal_highs(cls, window: DataFrame) -> list[int]:
        wing = cls.FRACTAL_WING
        high = window["high"]
        positions = []
        for i in range(wing, len(high) - wing):
            neighborhood = high.iloc[i - wing : i + wing + 1]
            if (
                high.iloc[i] == neighborhood.max()
                and (neighborhood == neighborhood.max()).sum() == 1
            ):
                positions.append(i)
        return positions

    @classmethod
    def detect(cls, df: DataFrame | None) -> dict[str, float | int] | None:
        if df is None or "close_time" not in df.columns:
            return None

        close_times = to_numeric(df["close_time"], errors="coerce")
        completed_candles = df.loc[close_times <= time() * 1000]
        if len(completed_candles) < cls.LOOKBACK_BARS:
            return None

        window = completed_candles.iloc[-cls.LOOKBACK_BARS :].reset_index(drop=True)
        trough_positions = cls._fractal_lows(window)
        if len(trough_positions) < 2:
            return None

        earlier_pos, later_pos = trough_positions[-2], trough_positions[-1]
        preceding_highs = [
            position
            for position in cls._fractal_highs(window)
            if position < earlier_pos
        ]
        if not preceding_highs:
            return None

        swing_high_pos = preceding_highs[-1]
        earlier_low = float(window["low"].iloc[earlier_pos])
        later_low = float(window["low"].iloc[later_pos])
        swing_high = float(window["high"].iloc[swing_high_pos])
        drop_pct = (1 - earlier_low / swing_high) * 100
        rise_pct = (later_low / earlier_low - 1) * 100
        if drop_pct < cls.MIN_DROP_PCT or rise_pct < cls.MIN_RISE_PCT:
            return None

        confirmation_pos = later_pos + cls.FRACTAL_WING
        return {
            "earlier_low": earlier_low,
            "later_low": later_low,
            "swing_high": swing_high,
            "drop_pct": drop_pct,
            "rise_pct": rise_pct,
            "swing_high_open_time": int(window["open_time"].iloc[swing_high_pos]),
            "later_open_time": int(window["open_time"].iloc[later_pos]),
            "confirmation_open_time": int(window["open_time"].iloc[confirmation_pos]),
        }

    def _already_emitted(self, open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_open_time == open_time
        return self.strategy_cooldowns.get((self.ALGO, self.symbol)) == open_time

    def _mark_emitted(self, open_time: int) -> None:
        self._last_emitted_open_time = open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = open_time

    def _cooldown_active(self, evaluation_time_ms: int) -> bool:
        if self.strategy_cooldowns is None:
            last_emitted_at = self._last_emitted_at
        else:
            last_emitted_at = self.strategy_cooldowns.get(
                (f"{self.ALGO}_alert_cooldown", self.symbol)
            )
        if last_emitted_at is None:
            return False

        cooldown_ms = self.ALERT_COOLDOWN_MINUTES * 60 * 1000
        return evaluation_time_ms - last_emitted_at < cooldown_ms

    def _mark_cooldown(self, evaluation_time_ms: int) -> None:
        self._last_emitted_at = evaluation_time_ms
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(f"{self.ALGO}_alert_cooldown", self.symbol)] = (
                evaluation_time_ms
            )

    async def signal(self) -> None:
        df = self.ti.df_15m
        evaluation_time = datetime.now(UTC)
        evaluation_time_ms = int(evaluation_time.timestamp() * 1000)
        pattern = self.detect(df)
        if pattern is None:
            return

        later_open_time = int(pattern["later_open_time"])
        if self._already_emitted(later_open_time) or self._cooldown_active(
            evaluation_time_ms
        ):
            return
        self._mark_emitted(later_open_time)
        self._mark_cooldown(evaluation_time_ms)

        current_price = float(df["close"].iloc[-1])
        msg = f"""
            - 📈 [{self.config.env}] <strong>#{self.ALGO} pattern</strong> #{self.symbol}
            - Event: higher low
            - First trough: {round(pattern["earlier_low"], self.price_precision)}
            - Second trough: {round(pattern["later_low"], self.price_precision)} ({round(pattern["rise_pct"], 2)}% above first trough)
            - Drop into first trough: {round(pattern["drop_pct"], 2)}% from swing high {round(pattern["swing_high"], self.price_precision)}
            - Current price: {round(current_price, self.price_precision)}
            - Evaluation time: {evaluation_time.strftime("%Y-%m-%d %H:%M:%S UTC")}
            {self.ti.regime_telegram_lines()}
            - Interpretation: downward momentum is fading; watch for a reversal or a consolidation range
            - Autotrade: disabled, notification only
            """

        self.telegram_consumer.dispatch_signal(msg)

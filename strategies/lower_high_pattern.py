from typing import TYPE_CHECKING

from pandas import DataFrame
from pybinbot import round_numbers

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LowerHighPattern:
    """
    Detects a lower-high price-structure pattern on 15m candles: after an
    uptrend forms a swing high, price pulls back and rallies again but the
    new swing high fails to exceed the prior one. This is a classic early
    signal that upward momentum is fading, often preceding a reversal or a
    consolidation range.

    Notification only: this strategy never builds a SignalsConsumer/BotBase
    payload and never reaches AutotradeConsumer, so no bot can be opened
    from it.

    Swing highs are found with a simple fractal rule: a candle's high must
    strictly exceed the highs of FRACTAL_WING candles on each side of it.
    Only the two most recent confirmed fractal highs in the lookback window
    are compared.
    """

    ALGO = "lower_high_pattern"

    FRACTAL_WING = 2
    LOOKBACK_BARS = 40
    # Minimum rise (%) from the swing low into the first peak: confirms the
    # first peak actually capped a genuine uptrend leg, not just noise.
    MIN_RISE_PCT = 2.0
    # Minimum drop (%) of the second peak below the first: filters out
    # peaks that are equal within noise (which would just be a double top).
    MIN_DROP_PCT = 0.1

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.symbol = cls.symbol
        self.config = cls.config
        self.telegram_consumer = cls.telegram_consumer
        self.price_precision = cls.price_precision
        self.strategy_cooldowns = cls.strategy_cooldowns
        self._last_emitted_open_time: int | None = None

    def _fractal_highs(self, window: DataFrame) -> list[int]:
        wing = self.FRACTAL_WING
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

    def _already_emitted(self, open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_open_time == open_time
        return self.strategy_cooldowns.get((self.ALGO, self.symbol)) == open_time

    def _mark_emitted(self, open_time: int) -> None:
        self._last_emitted_open_time = open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = open_time

    async def signal(self) -> None:
        df = self.ti.df_15m
        if df is None or len(df) < self.LOOKBACK_BARS:
            return

        window = df.iloc[-self.LOOKBACK_BARS :].reset_index(drop=True)
        peak_positions = self._fractal_highs(window)
        if len(peak_positions) < 2:
            return

        earlier_pos, later_pos = peak_positions[-2], peak_positions[-1]
        earlier_high = float(window["high"].iloc[earlier_pos])
        later_high = float(window["high"].iloc[later_pos])

        swing_low = float(window["low"].iloc[: earlier_pos + 1].min())
        rise_pct = (earlier_high / swing_low - 1) * 100
        drop_pct = (1 - later_high / earlier_high) * 100

        if rise_pct < self.MIN_RISE_PCT or drop_pct < self.MIN_DROP_PCT:
            return

        later_open_time = int(window["open_time"].iloc[later_pos])
        if self._already_emitted(later_open_time):
            return
        self._mark_emitted(later_open_time)

        current_price = float(df["close"].iloc[-1])
        precision = self.price_precision
        msg = f"""
            - 📉 [{self.config.env}] <strong>#{self.ALGO} pattern</strong> #{self.symbol}
            - Event: lower high
            - First peak: {round_numbers(earlier_high, precision)}
            - Second peak: {round_numbers(later_high, precision)} ({round_numbers(drop_pct, 2)}% below first peak)
            - Rise into first peak: {round_numbers(rise_pct, 2)}% from swing low {round_numbers(swing_low, precision)}
            - Current price: {round_numbers(current_price, precision)}
            - Interpretation: upward momentum is fading; watch for a reversal or a consolidation range
            - Autotrade: disabled, notification only
            """

        self.telegram_consumer.dispatch_signal(msg)

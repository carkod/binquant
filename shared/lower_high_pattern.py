from time import time

from pandas import DataFrame, to_numeric

from shared.trend import regression_slope


class LowerHighPattern:
    """
    Detects a lower-high price-structure pattern on 15m candles: after an
    uptrend forms a swing high, price pulls back and rallies again but the
    new swing high fails to exceed the prior one. This is a classic early
    signal that upward momentum is fading, often preceding a reversal or a
    consolidation range.

    Swing highs are found with a simple fractal rule: a candle's high must
    strictly exceed the highs of FRACTAL_WING candles on each side of it.
    Only the two most recent confirmed fractal highs in the lookback window
    are compared.
    """

    FRACTAL_WING = 2
    LOOKBACK_BARS = 40
    # Minimum rise (%) from the swing low into the first peak: confirms the
    # first peak actually capped a genuine uptrend leg, not just noise.
    MIN_RISE_PCT = 2.0
    # Require strictly more than 5% separation and a downward regression
    # across the full completed lookback, so two wicks cannot define a trend.
    MIN_DROP_PCT = 5.0

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
    def detect(cls, df: DataFrame | None) -> dict[str, float | int] | None:
        if df is None or "close_time" not in df.columns:
            return None

        close_times = to_numeric(df["close_time"], errors="coerce")
        completed_candles = df.loc[close_times <= time() * 1000]
        if len(completed_candles) < cls.LOOKBACK_BARS:
            return None

        window = completed_candles.iloc[-cls.LOOKBACK_BARS :].reset_index(drop=True)
        peak_positions = cls._fractal_highs(window)
        if len(peak_positions) < 2:
            return None

        earlier_pos, later_pos = peak_positions[-2], peak_positions[-1]
        preceding_lows = [
            position for position in cls._fractal_lows(window) if position < earlier_pos
        ]
        if not preceding_lows:
            return None

        swing_low_pos = preceding_lows[-1]
        earlier_high = float(window["high"].iloc[earlier_pos])
        later_high = float(window["high"].iloc[later_pos])
        swing_low = float(window["low"].iloc[swing_low_pos])
        rise_pct = (earlier_high / swing_low - 1) * 100
        drop_pct = (1 - later_high / earlier_high) * 100
        if rise_pct < cls.MIN_RISE_PCT or later_high >= earlier_high * (
            1 - cls.MIN_DROP_PCT / 100
        ):
            return None
        slope = regression_slope(
            [
                (float(timestamp) / 1000, float(close))
                for timestamp, close in zip(
                    window["open_time"], window["close"], strict=True
                )
            ]
        )
        if slope is None or slope >= 0:
            return None

        confirmation_pos = later_pos + cls.FRACTAL_WING
        return {
            "earlier_high": earlier_high,
            "later_high": later_high,
            "swing_low": swing_low,
            "rise_pct": rise_pct,
            "drop_pct": drop_pct,
            "trend_slope_per_hour": slope,
            "swing_low_open_time": int(window["open_time"].iloc[swing_low_pos]),
            "later_open_time": int(window["open_time"].iloc[later_pos]),
            "confirmation_open_time": int(window["open_time"].iloc[confirmation_pos]),
        }

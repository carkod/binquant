import pandas as pd
import pytest

from shared.lower_high_pattern import LowerHighPattern

BASE_OPEN_TIME_MS = 1_700_000_000_000
BAR_MS = 15 * 60 * 1000


def make_lower_high_df(*, second_peak_high: float = 132) -> pd.DataFrame:
    """
    40 x 15m bars shaping a lower high: a clean uptrend into a first swing
    high (bar 14, high=140), a pullback to a trough (bar 17), a second rally
    into a swing high at bar 21 (default 132, i.e. below 140), then a clean
    decline. Passing a higher `second_peak_high` turns bar 21 into a higher
    high instead.
    """
    highs: list[float] = [100 + i * 2 for i in range(14)]  # bars 0-13: 100 -> 126
    highs.append(140)  # bar 14: first swing high
    highs.extend([132, 124, 116, 122, 128, 131])  # bars 15-20: pullback + rally
    highs.append(second_peak_high)  # bar 21: second swing high
    highs.extend(130 - i * 6 for i in range(18))  # bars 22-39: decline

    lows = [h - 2 for h in highs]
    lows[8] = 109
    closes = [h - 1 for h in highs]
    open_times = [BASE_OPEN_TIME_MS + i * BAR_MS for i in range(len(highs))]

    return pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": closes,
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )


def make_flat_uptrend_df() -> pd.DataFrame:
    """40 bars of a strictly increasing price: no fractal swing high forms."""
    highs = [100 + i for i in range(40)]
    lows = [h - 2 for h in highs]
    closes = [h - 1 for h in highs]
    open_times = [BASE_OPEN_TIME_MS + i * BAR_MS for i in range(40)]
    return pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": closes,
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )


def test_lower_high_pattern_detects_confirmed_lower_high():
    pattern = LowerHighPattern.detect(make_lower_high_df())

    assert pattern is not None
    assert pattern["earlier_high"] == 140
    assert pattern["later_high"] == 132
    assert pattern["later_open_time"] == BASE_OPEN_TIME_MS + 21 * BAR_MS
    assert pattern["confirmation_open_time"] == BASE_OPEN_TIME_MS + 23 * BAR_MS


def test_lower_high_pattern_skips_when_second_peak_is_higher():
    assert LowerHighPattern.detect(make_lower_high_df(second_peak_high=150)) is None


def test_lower_high_pattern_skips_drop_below_five_percent():
    assert LowerHighPattern.detect(make_lower_high_df(second_peak_high=136.6)) is None


@pytest.mark.parametrize(
    "second_peak,passes", [(133.01, False), (133, False), (132.99, True)]
)
def test_lower_high_pattern_requires_drop_strictly_above_five_percent(
    second_peak, passes
):
    pattern = LowerHighPattern.detect(make_lower_high_df(second_peak_high=second_peak))

    assert (pattern is not None) is passes
    if pattern is not None:
        assert pattern["drop_pct"] > 5
        assert pattern["trend_slope_per_hour"] < 0


@pytest.mark.parametrize("slope", [0, 1])
def test_lower_high_pattern_rejects_flat_or_rising_full_window(slope):
    frame = make_lower_high_df()
    frame["close"] = [50 + slope * i for i in range(len(frame))]

    assert LowerHighPattern.detect(frame) is None


def test_lower_high_pattern_skips_when_fewer_than_two_swing_highs():
    assert LowerHighPattern.detect(make_flat_uptrend_df()) is None


def test_lower_high_pattern_uses_nearest_preceding_fractal_low():
    frame = make_lower_high_df()
    frame.loc[2, "low"] = 80
    frame.loc[8, "low"] = 95

    pattern = LowerHighPattern.detect(frame)

    assert pattern is not None
    assert pattern["swing_low"] == 95
    assert pattern["swing_low_open_time"] == BASE_OPEN_TIME_MS + 8 * BAR_MS


def test_lower_high_pattern_waits_for_confirmation_candle_to_close():
    highs = [99.0] + [100.0 + index for index in range(30)]
    highs.extend([140.0, 132.0, 124.0, 120.0, 125.0, 130.0, 133.0, 136.0, 130.0, 124.0])
    lows = [high - 2 for high in highs]
    lows[20] = 114
    open_times = [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(len(highs))]
    frame = pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": [high - 1 for high in highs],
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )
    frame.loc[frame.index[-1], "close_time"] = 10**15
    assert LowerHighPattern.detect(frame) is None

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pandas as pd
import pytest

from strategies.higher_low_pattern import HigherLowPattern

BASE_OPEN_TIME_MS = 1_700_000_000_000
BAR_MS = 15 * 60 * 1000


def make_higher_low_df(*, second_trough_low: float = 104) -> pd.DataFrame:
    """
    40 x 15m bars shaping a higher low: a clean downtrend into a first swing
    low (bar 14, low=100), a bounce to a peak (bar 17), a second dip into a
    swing low at bar 21 (default 104, i.e. above 100), then a clean rally.
    Passing a lower `second_trough_low` turns bar 21 into a lower low
    instead.
    """
    lows: list[float] = [140 - i * 2 for i in range(14)]  # bars 0-13: 140 -> 114
    lows.append(100)  # bar 14: first swing low
    lows.extend([108, 116, 124, 118, 112, 107])  # bars 15-20: bounce + dip
    lows.append(second_trough_low)  # bar 21: second swing low
    lows.extend(110 + i * 6 for i in range(18))  # bars 22-39: rally

    highs = [low + 2 for low in lows]
    highs[8] = 131
    closes = [low + 1 for low in lows]
    open_times = [BASE_OPEN_TIME_MS + i * BAR_MS for i in range(len(lows))]

    return pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": closes,
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )


def make_flat_downtrend_df() -> pd.DataFrame:
    """40 bars of a strictly decreasing price: no fractal swing low forms."""
    lows = [140 - i for i in range(40)]
    highs = [low + 2 for low in lows]
    closes = [low + 1 for low in lows]
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


def make_algo(
    df: pd.DataFrame,
    *,
    strategy_cooldowns: dict | None = None,
    price_precision: int = 4,
) -> HigherLowPattern:
    cls = SimpleNamespace(
        symbol="TESTUSDT",
        config=SimpleNamespace(env="test"),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        price_precision=price_precision,
        strategy_cooldowns=strategy_cooldowns,
        df_15m=df,
    )
    return HigherLowPattern(cast(Any, cls))


@pytest.mark.asyncio
async def test_higher_low_pattern_emits_on_confirmed_higher_low():
    algo = make_algo(make_higher_low_df(), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]
    msg = algo.telegram_consumer.dispatch_signal.call_args.args[0]  # type: ignore[attr-defined]
    assert "higher low" in msg
    assert "100" in msg
    assert "104" in msg
    assert "Autotrade: disabled" in msg


@pytest.mark.asyncio
async def test_higher_low_pattern_skips_when_second_trough_is_lower():
    algo = make_algo(make_higher_low_df(second_trough_low=90), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_higher_low_pattern_skips_sub_half_percent_rise():
    algo = make_algo(make_higher_low_df(second_trough_low=100.4), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_higher_low_pattern_skips_when_fewer_than_two_swing_lows():
    algo = make_algo(make_flat_downtrend_df(), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_higher_low_pattern_deduplicates_same_swing_low_across_instances():
    df = make_higher_low_df()
    shared_cooldowns: dict = {}

    first = make_algo(df, strategy_cooldowns=shared_cooldowns)
    await first.signal()
    first.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]

    second = make_algo(df, strategy_cooldowns=shared_cooldowns)
    await second.signal()
    second.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


def test_higher_low_pattern_uses_nearest_preceding_fractal_high():
    frame = make_higher_low_df()
    frame.loc[2, "high"] = 160
    frame.loc[8, "high"] = 135

    pattern = HigherLowPattern.detect(frame)

    assert pattern is not None
    assert pattern["swing_high"] == 135
    assert pattern["swing_high_open_time"] == BASE_OPEN_TIME_MS + 8 * BAR_MS


@pytest.mark.asyncio
async def test_higher_low_pattern_rounds_prices_and_includes_evaluation_time(
    monkeypatch: pytest.MonkeyPatch,
):
    evaluation_time = datetime(2026, 9, 27, 14, 5, 6, tzinfo=UTC)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return evaluation_time

    monkeypatch.setattr("strategies.higher_low_pattern.datetime", FrozenDateTime)
    frame = make_higher_low_df(second_trough_low=104.12346)
    frame.loc[14, "low"] = 100.12346
    frame.loc[8, "high"] = 135.12346
    frame.loc[frame.index[-1], "close"] = 224.12346
    algo = make_algo(frame, strategy_cooldowns={}, price_precision=4)

    await algo.signal()

    msg = algo.telegram_consumer.dispatch_signal.call_args.args[0]  # type: ignore[attr-defined]
    assert "First trough: 100.1235" in msg
    assert "Second trough: 104.1235" in msg
    assert "swing high 135.1235" in msg
    assert "Current price: 224.1235" in msg
    assert "Evaluation time: 2026-09-27 14:05:06 UTC" in msg


@pytest.mark.asyncio
async def test_higher_low_pattern_applies_four_hour_symbol_cooldown(
    monkeypatch: pytest.MonkeyPatch,
):
    first_evaluation = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)

    class FrozenDateTime(datetime):
        current = first_evaluation

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr("strategies.higher_low_pattern.datetime", FrozenDateTime)
    shared_cooldowns: dict = {}
    first = make_algo(make_higher_low_df(), strategy_cooldowns=shared_cooldowns)

    await first.signal()

    first.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]

    shifted_frame = make_higher_low_df()
    shifted_frame["open_time"] += BAR_MS
    shifted_frame["close_time"] += BAR_MS
    FrozenDateTime.current = first_evaluation + timedelta(hours=3, minutes=59)
    still_cooling_down = make_algo(
        shifted_frame,
        strategy_cooldowns=shared_cooldowns,
    )

    await still_cooling_down.signal()

    still_cooling_down.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]

    FrozenDateTime.current = first_evaluation + timedelta(hours=4)
    cooldown_elapsed = make_algo(
        shifted_frame,
        strategy_cooldowns=shared_cooldowns,
    )

    await cooldown_elapsed.signal()

    cooldown_elapsed.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_higher_low_pattern_waits_for_confirmation_candle_to_close():
    lows = [141.0] + [140.0 - index for index in range(30)]
    lows.extend([100.0, 108.0, 116.0, 120.0, 115.0, 110.0, 107.0, 104.0, 110.0, 116.0])
    highs = [low + 2 for low in lows]
    highs[20] = 126
    open_times = [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(len(lows))]
    frame = pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": [low + 1 for low in lows],
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )
    frame.loc[frame.index[-1], "close_time"] = 10**15
    algo = make_algo(frame, strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]

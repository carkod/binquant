from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pandas as pd
import pytest

from strategies.lower_high_pattern import LowerHighPattern

BASE_OPEN_TIME_MS = 1_700_000_000_000
BAR_MS = 15 * 60 * 1000


def make_lower_high_df(*, second_peak_high: float = 136) -> pd.DataFrame:
    """
    40 x 15m bars shaping a lower high: a clean uptrend into a first swing
    high (bar 14, high=140), a pullback to a trough (bar 17), a second rally
    into a swing high at bar 21 (default 136, i.e. below 140), then a clean
    decline. Passing a higher `second_peak_high` turns bar 21 into a higher
    high instead.
    """
    highs: list[float] = [100 + i * 2 for i in range(14)]  # bars 0-13: 100 -> 126
    highs.append(140)  # bar 14: first swing high
    highs.extend([132, 124, 116, 122, 128, 133])  # bars 15-20: pullback + rally
    highs.append(second_peak_high)  # bar 21: second swing high
    highs.extend(130 - i * 6 for i in range(18))  # bars 22-39: decline

    lows = [h - 2 for h in highs]
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


def make_algo(
    df: pd.DataFrame,
    *,
    strategy_cooldowns: dict | None = None,
) -> LowerHighPattern:
    cls = SimpleNamespace(
        symbol="TESTUSDT",
        config=SimpleNamespace(env="test"),
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        price_precision=4,
        strategy_cooldowns=strategy_cooldowns,
        df_15m=df,
    )
    return LowerHighPattern(cast(Any, cls))


@pytest.mark.asyncio
async def test_lower_high_pattern_emits_on_confirmed_lower_high():
    algo = make_algo(make_lower_high_df(), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]
    msg = algo.telegram_consumer.dispatch_signal.call_args.args[0]  # type: ignore[attr-defined]
    assert "lower high" in msg
    assert "140" in msg
    assert "136" in msg
    assert "Autotrade: disabled" in msg


@pytest.mark.asyncio
async def test_lower_high_pattern_skips_when_second_peak_is_higher():
    algo = make_algo(make_lower_high_df(second_peak_high=150), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_lower_high_pattern_skips_when_fewer_than_two_swing_highs():
    algo = make_algo(make_flat_uptrend_df(), strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_lower_high_pattern_deduplicates_same_swing_high_across_instances():
    df = make_lower_high_df()
    shared_cooldowns: dict = {}

    first = make_algo(df, strategy_cooldowns=shared_cooldowns)
    await first.signal()
    first.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]

    second = make_algo(df, strategy_cooldowns=shared_cooldowns)
    await second.signal()
    second.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_lower_high_pattern_waits_for_confirmation_candle_to_close():
    highs = [99.0] + [100.0 + index for index in range(30)]
    highs.extend([140.0, 132.0, 124.0, 120.0, 125.0, 130.0, 133.0, 136.0, 130.0, 124.0])
    open_times = [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(len(highs))]
    frame = pd.DataFrame(
        {
            "high": highs,
            "low": [high - 2 for high in highs],
            "close": [high - 1 for high in highs],
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )
    frame.loc[frame.index[-1], "close_time"] = 10**15
    algo = make_algo(frame, strategy_cooldowns={})

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]

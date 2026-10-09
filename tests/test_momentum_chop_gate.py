from typing import Any, cast
from unittest.mock import Mock

import pytest

from shared.price_crossings import price_crossings_six_hours
from strategies.top_gainer_early_momentum import TopGainerEarlyMomentum
from strategies.top_loser_early_momentum import TopLoserEarlyMomentum
from tests.test_top_gainer_early_momentum import (
    make_breakout_candles,
    make_context as make_gainer_context,
    make_market_context,
)
from tests.test_top_loser_early_momentum import (
    make_breakdown_candles,
    make_context as make_loser_context,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize(
    "case",
    ["two_crossings", "three_crossings", "missing", "gap", "stale", "nan", "forming"],
)
async def test_momentum_chop_gate_before_dispatch_and_cooldown(
    monkeypatch, direction: str, case: str
) -> None:
    strategy: TopGainerEarlyMomentum | TopLoserEarlyMomentum
    if direction == "long":
        frame = make_breakout_candles()
        context = make_gainer_context(
            df_15m=frame, latest_market_context=make_market_context()
        )
        strategy = TopGainerEarlyMomentum(cast(Any, context))
    else:
        frame = make_breakdown_candles()
        context = make_loser_context(frame)
        strategy = TopLoserEarlyMomentum(cast(Any, context))

    current_price = float(frame.iloc[-1]["close"])
    now_ms = int(frame.iloc[-1]["close_time"]) + 60_001
    # Freeze both candle filtering and the new gate at the same evaluation time.
    clock = Mock()
    clock.now.return_value.timestamp.return_value = now_ms / 1000
    monkeypatch.setattr(f"{strategy.__module__}.datetime", clock)
    values, reason = strategy._features(frame.iloc[:-2])
    assert values is not None
    assert strategy._entry_allows(values)[0]
    # Hold the original impulse fixed while testing later six-hour price churn.
    monkeypatch.setattr(strategy, "_features", lambda _: (values, reason))

    if case in {"two_crossings", "three_crossings"}:
        initial_side = -1 if direction == "long" else 1
        if case == "three_crossings":
            initial_side *= -1
        closes = [current_price + initial_side] * 7
        closes += [current_price - initial_side] * 7
        closes += [current_price + initial_side] * 7
        for index, close in zip(frame.index[-24:-3], closes, strict=True):
            frame.loc[index, ["open", "high", "low", "close"]] = [
                close,
                close + 0.25,
                close - 0.25,
                close,
            ]
        assert price_crossings_six_hours(frame, current_price, now_ms=now_ms) == (
            2 if case == "two_crossings" else 3
        )
    elif case == "missing":
        context.df_15m = frame.iloc[-23:]
    elif case == "gap":
        context.df_15m = frame.drop(frame.index[-10])
    elif case == "stale":
        frame["open_time"] -= 900_000
        frame["close_time"] -= 900_000
    elif case == "nan":
        frame.loc[frame.index[-10], "close"] = float("nan")
    elif case == "forming":
        # A forming candle on the other side cannot add another crossing.
        live = frame.iloc[-1].copy()
        live["open_time"] += 900_000
        live["close_time"] += 900_000
        live["close"] = current_price + (1 if direction == "long" else -1)
        frame.loc[len(frame)] = live

    await strategy.signal(
        current_price, current_price + 5, current_price, current_price - 5
    )

    if case in {"two_crossings", "forming"}:
        context.dispatch_signal_record.assert_awaited_once()
        context.telegram_consumer.dispatch_signal.assert_awaited_once()
        indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
        expected = 2 if case == "two_crossings" else 0
        assert indicators["price_crossings_six_hours"] == expected
        assert indicators["chop_crossing_threshold"] == 3
        assert indicators["chop_lookback_bars"] == 24
        message = context.telegram_consumer.dispatch_signal.await_args.args[0]
        assert f"Current-price crossings over 6h: {expected}; blocked at 3" in message
        context.at_consumer.process_autotrade_restrictions.assert_awaited_once()
    else:
        context.dispatch_signal_record.assert_not_awaited()
        context.telegram_consumer.dispatch_signal.assert_not_awaited()
        context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
        assert context.strategy_cooldowns == {}

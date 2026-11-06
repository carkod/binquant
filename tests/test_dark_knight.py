from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pandas as pd
import pytest

from strategies.dark_knight import (
    DarkKnight,
    breadth_crossings_six_hours,
    completed_ohlcv,
    leverage_tier,
    score_short_setup,
)
from tests.test_top_gainer_short import (
    BAR_MS,
    NOW,
    make_context,
    make_market_breadth,
    make_top_gainers,
)


def frame_with_volume() -> pd.DataFrame:
    frame = make_context().df_15m.copy()
    frame["open"] = frame.close + 0.02
    frame["volume"] = 100.0
    return frame


def context() -> SimpleNamespace:
    result = make_context(symbol_df=frame_with_volume())
    result.df_1h = None
    result.microregime_directional = None
    result.macroregime_directional = None

    async def persist(*, value, indicators):  # noqa: ARG001
        value.bot_params.signal_id = uuid4()

    result.dispatch_signal_record = AsyncMock(side_effect=persist)
    return result


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr("strategies.dark_knight.time", lambda: NOW.timestamp())
    monkeypatch.setattr("strategies.lower_high_pattern.time", lambda: NOW.timestamp())


def evidence(frame=None, **kwargs):
    inputs: dict[str, Any] = {
        "current_price": 135.0,
        "earlier_peak": 30,
        "later_peak": 37,
        "btc": None,
        "hourly": None,
        "daily": None,
        "micro_direction": None,
        "crossings": 0,
    }
    inputs.update(kwargs)
    return score_short_setup(
        frame if frame is not None else frame_with_volume(), **inputs
    )


@pytest.mark.parametrize(
    "score,leverage",
    [
        (-1, 1),
        (0, 1),
        (34.9999, 1),
        (35, 2),
        (49.9999, 2),
        (50, 3),
        (64.9999, 3),
        (65, 4),
        (79.9999, 4),
        (80, 5),
        (100, 5),
        (101, 5),
    ],
)
def test_proposed_leverage_has_explicit_inclusive_quality_bands(score, leverage):
    assert leverage_tier(score)[0] == leverage


@pytest.mark.parametrize("score", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_quality_cannot_propose_leverage(score):
    with pytest.raises(ValueError):
        leverage_tier(score)


def test_lower_high_without_support_break_does_not_claim_breakdown():
    result = evidence()
    assert result["intervening_support"] == 135.05
    assert result["entry_reason"] == "confirmed_lower_high"
    assert result["score_components"]["structure"] <= 10
    assert result["btc_return_1h"] is None


def test_two_closes_below_support_add_confirmation_but_reclaim_removes_it():
    frame = frame_with_volume()
    frame.loc[38:, ["open", "high", "low", "close"]] = [135, 135.2, 132, 133]
    result = evidence(frame, current_price=133)
    assert result["support_broken"] is True
    assert result["breakdown_held_two_closes"] is True
    assert result["score_components"]["structure"] > 20
    reclaimed = evidence(frame, current_price=135.1)
    assert not reclaimed["support_broken"]
    assert not reclaimed["breakdown_held_two_closes"]
    assert reclaimed["score_components"]["structure"] == pytest.approx(
        result["score_components"]["structure"] - 20
    )


def test_bearish_volume_increases_selling_score_but_bullish_volume_does_not():
    frame = frame_with_volume()
    frame.loc[39, ["open", "high", "low", "close"]] = [135, 135.2, 132, 133]
    base = evidence(frame)["score_components"]["selling_pressure"]
    frame.loc[39, "volume"] = 200
    assert evidence(frame)["score_components"]["selling_pressure"] == pytest.approx(
        base + 10
    )
    frame.loc[39, ["open", "close"]] = [133, 135]
    assert evidence(frame)["score_components"]["selling_pressure"] == 0


def test_relative_weakness_requires_coin_falling_while_btc_recovers():
    frame = frame_with_volume()
    btc = frame.copy()
    frame.loc[35:, "close"] = [135, 134, 133, 132, 131]
    btc.loc[35:, "close"] = [100, 101, 102, 103, 104]
    assert evidence(frame, btc=btc)["score_components"]["relative_weakness"] > 0
    btc.loc[35:, "close"] = [104, 103, 102, 101, 100]
    assert evidence(frame, btc=btc)["score_components"]["relative_weakness"] == 0


def test_btc_candles_must_align_with_coin_before_awarding_relative_points():
    frame = frame_with_volume()
    btc = frame.copy()
    btc["open_time"] += 1
    assert evidence(frame, btc=btc)["btc_return_1h"] is None


def test_quality_and_downside_room_are_independent():
    first = evidence(current_price=135)
    second = evidence(current_price=135.01)
    assert first["quality_score"] == second["quality_score"]
    assert first["downside_room_pct"] != second["downside_room_pct"]
    assert first["next_support"] == 105


def test_absent_lower_support_is_unknown_not_invented_target():
    frame = frame_with_volume()
    frame.loc[10, "low"] = 135.05
    result = evidence(frame, current_price=135)
    assert result["next_support"] is None
    assert result["downside_room_pct"] is None
    assert result["downside_room_tier"] == "unknown"


def test_chop_and_late_entry_reduce_quality_without_exceeding_bounds():
    base = evidence()
    chopped = evidence(crossings=2)
    assert chopped["quality_score"] == pytest.approx(base["quality_score"] - 10)
    late = evidence(current_price=100)
    assert late["score_penalties"]["late_entry"] == 15
    assert 0 <= late["quality_score"] < base["quality_score"] <= 100


def test_missing_optional_evidence_is_zero_points_not_rescaled():
    result = evidence()
    for name in ("relative_weakness", "timeframe_agreement", "prior_extension"):
        assert result["score_components"][name] == 0
    assert not any(result["optional_data_available"].values())
    assert result["quality_score"] == pytest.approx(
        sum(result["score_components"].values())
    )


def test_daily_extension_has_five_point_cap():
    daily = frame_with_volume().tail(21).copy()
    daily["close"] = 50.0
    result = evidence(daily=daily)
    assert result["score_components"]["prior_extension"] == 5
    daily.loc[daily.index[-7:], "close"] = 10000.0
    assert evidence(daily=daily)["daily_extension_pct"] == result["daily_extension_pct"]


def test_breadth_crossings_count_signed_values_over_six_hours():
    latest = NOW.timestamp() - 5 * 60
    samples = [
        (latest - (24 - index) * 15 * 60, -1.0 if index % 2 == 0 else 1.0)
        for index in range(24)
    ] + [(latest, 0.0)]

    assert breadth_crossings_six_hours(samples, now_seconds=NOW.timestamp()) == 23
    assert (
        breadth_crossings_six_hours(samples[:23], now_seconds=NOW.timestamp()) is None
    )


@pytest.mark.parametrize(
    "case",
    [
        "gap",
        "duplicate",
        "stale",
        "nan",
        "negative_volume",
        "bad_ohlc",
        "bad_duration",
        "short",
    ],
)
def test_invalid_candle_history_cannot_be_scored(case):
    frame = frame_with_volume()
    if case == "gap":
        frame.loc[10, "open_time"] = 0
    elif case == "duplicate":
        frame.loc[10, "open_time"] = frame.loc[9, "open_time"]
    elif case == "stale":
        frame[["open_time", "close_time"]] -= BAR_MS
    elif case == "nan":
        frame.loc[10, "volume"] = float("nan")
    elif case == "negative_volume":
        frame.loc[10, "volume"] = -1
    elif case == "bad_ohlc":
        frame.loc[10, "close"] = 999
    elif case == "bad_duration":
        frame.loc[10, "close_time"] = 0
    elif case == "short":
        frame = frame.iloc[1:]
    assert (
        completed_ohlcv(
            frame, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS, bars=40
        )
        is None
    )


def test_forming_candle_does_not_change_evidence_and_input_order_does_not_matter():
    frame = frame_with_volume()
    forming = frame.tail(1).copy()
    forming[["open_time", "close_time"]] += BAR_MS
    forming["low"] = 1
    combined = pd.concat([frame, forming]).iloc[::-1]
    completed = completed_ohlcv(
        combined, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS, bars=40
    )
    assert completed is not None
    pd.testing.assert_frame_equal(completed, frame, check_like=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("environment", ["development", "staging", "production"])
async def test_persists_proposed_leverage_and_never_requests_autotrade(environment):
    ctx = context()
    ctx.config.env = environment
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_awaited_once()
    value = ctx.dispatch_signal_record.await_args.kwargs["value"]
    indicators = ctx.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.autotrade is False
    assert value.bot_params.name == "dark_knight"
    assert value.bot_params.futures_leverage == indicators["proposed_futures_leverage"]
    assert (
        value.bot_params.fiat_order_size
        == ctx.at_consumer.autotrade_settings.base_order_size
    )
    assert 1 <= value.bot_params.futures_leverage <= 5
    assert value.score == indicators["quality_score"]
    assert indicators["observation_only"] is True
    assert indicators["stop_loss_price_at_signal"] > 136
    assert value.bot_params.stop_loss == pytest.approx(2.7556)
    ctx.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    ctx.finalize_signal_bot_params.assert_not_called()
    message = ctx.telegram_consumer.dispatch_signal.await_args.args[0]
    assert "#dark_knight algorithm" in message
    assert "Proposed futures leverage" in message
    assert "Six-hour crossings: coin" in message
    assert "calibration observations only" in message


@pytest.mark.asyncio
async def test_observation_dedupe_is_independent_from_top_gainer_short():
    ctx = context()
    ctx.strategy_cooldowns[("top_gainer_short", ctx.symbol)] = int(
        ctx.df_15m.open_time.iloc[-1]
    )
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_awaited_once()
    assert ("dark_knight", ctx.symbol) in ctx.strategy_cooldowns


@pytest.mark.asyncio
async def test_persistence_failure_allows_retry_without_unrecorded_alert():
    ctx = context()
    ctx.dispatch_signal_record = AsyncMock()
    strategy = DarkKnight(cast(Any, ctx))
    await strategy.signal(135, 140, 132, 120)
    await strategy.signal(135, 140, 132, 120)
    assert ctx.dispatch_signal_record.await_count == 2
    assert ctx.strategy_cooldowns == {}
    ctx.telegram_consumer.dispatch_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_telegram_failure_does_not_duplicate_persisted_observation():
    ctx = context()
    ctx.telegram_consumer.dispatch_signal.side_effect = RuntimeError("unavailable")
    strategy = DarkKnight(cast(Any, ctx))
    with pytest.raises(RuntimeError):
        await strategy.signal(135, 140, 132, 120)
    await strategy.signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf"), 136])
async def test_invalid_entry_or_reclaimed_peak_cannot_emit(price):
    ctx = context()
    await DarkKnight(cast(Any, ctx)).signal(price, 140, 132, 120)
    ctx.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_btc_or_breadth_higher_low_blocks_observation():
    ctx = context()
    ctx.market_breadth_data = make_market_breadth(breadth=list(range(40)))
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_top_gainer_rank_does_not_change_dark_knight_quality():
    ctx = context()
    ctx.gainers_losers_series = make_top_gainers(recorded_at=NOW)
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    result = ctx.dispatch_signal_record.await_args.kwargs["indicators"]
    assert result["score_components"]["prior_extension"] == 0
    assert "top_gainer_gain_pct" not in result
    assert result["scoring_version"] == "dark_knight_v2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "btc_crossings,breadth_crossings,emits",
    [(3, 3, False), (2, 3, True), (3, 2, True), (None, 3, True), (3, None, True)],
)
async def test_only_joint_btc_and_breadth_chop_blocks_observation(
    monkeypatch, btc_crossings, breadth_crossings, emits
):
    ctx = context()
    ctx.df_btc_15m = frame_with_volume()
    monkeypatch.setattr(
        "strategies.dark_knight.price_crossings_six_hours",
        lambda df, price, **kwargs: 0 if price == 135 else btc_crossings,
    )
    monkeypatch.setattr(
        "strategies.dark_knight.breadth_crossings_six_hours",
        lambda samples, **kwargs: breadth_crossings,
    )

    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)

    if emits:
        ctx.dispatch_signal_record.assert_awaited_once()
        indicators = ctx.dispatch_signal_record.await_args.kwargs["indicators"]
        assert indicators["btc_crossings_six_hours"] == btc_crossings
        assert indicators["breadth_crossings_six_hours"] == breadth_crossings
    else:
        ctx.dispatch_signal_record.assert_not_awaited()


def test_hourly_lower_highs_and_lower_lows_add_timeframe_agreement():
    hourly = pd.DataFrame(
        {
            "high": [100, 101, 110, 101, 100, 99, 100, 105, 100, 99, 98, 99, 100],
            "low": [98, 97, 98, 97, 95, 90, 94, 95, 94, 90, 85, 90, 95],
        }
    )
    result = evidence(hourly=hourly, micro_direction="DOWN")
    assert result["score_components"]["timeframe_agreement"] == 15
    assert result["hourly_bearish_structure"] is True


def test_price_denomination_does_not_change_quality_or_downside_percentage():
    frame = frame_with_volume()
    scaled = frame.copy()
    scaled[["open", "high", "low", "close"]] *= 0.001
    first = evidence(frame)
    second = evidence(scaled, current_price=0.135)
    assert second["quality_score"] == pytest.approx(first["quality_score"])
    assert second["downside_room_pct"] == pytest.approx(first["downside_room_pct"])


@pytest.mark.asyncio
async def test_valid_btc_higher_low_can_confirm_without_breadth():
    from tests.test_top_gainer_short import make_btc_higher_low_df

    ctx = context()
    ctx.market_breadth_data = None
    ctx.df_btc_15m = make_btc_higher_low_df()
    ctx.df_btc_15m["open"] = ctx.df_btc_15m.close
    ctx.df_btc_15m["volume"] = 100.0
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_awaited_once()
    data = ctx.dispatch_signal_record.await_args.kwargs["indicators"]
    assert data["btc_higher_low"] is not None
    assert data["breadth_higher_low"] is None


@pytest.mark.asyncio
async def test_three_current_price_crossings_reject_observation(monkeypatch):
    ctx = context()
    monkeypatch.setattr(
        "strategies.dark_knight.price_crossings_six_hours", lambda *args, **kwargs: 3
    )
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_lower_high_confirmation_does_not_emit_again():
    ctx = context()
    extra = ctx.df_15m.tail(1).copy()
    extra[["open_time", "close_time"]] += BAR_MS
    ctx.df_15m = pd.concat([ctx.df_15m, extra], ignore_index=True)
    ctx.df_15m[["open_time", "close_time"]] -= BAR_MS
    await DarkKnight(cast(Any, ctx)).signal(135, 140, 132, 120)
    ctx.dispatch_signal_record.assert_not_awaited()

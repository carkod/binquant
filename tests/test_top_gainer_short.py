from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest
from pybinbot import (
    AutotradeSettingsSchema,
    ExchangeId,
    GainerLoserEntry,
    GainersLosersSnapshot,
    MarketBreadthSeries,
    MarketType,
    SymbolModel,
)

from shared.price_crossings import price_crossings_six_hours
from strategies.top_gainer_short import TopGainerShort

NOW = datetime(2026, 9, 23, 10, 20, tzinfo=UTC)
HIGHER_LOW_BREADTH = [0.3] * 30 + [
    -0.1,
    -0.3,
    -0.5,
    -0.3,
    -0.1,
    0.0,
    -0.2,
    -0.3,
    -0.1,
    0.0,
]

BAR_MS = 15 * 60 * 1000
BASE_OPEN_TIME_MS = int(NOW.timestamp() * 1000) // BAR_MS * BAR_MS - 40 * BAR_MS


def make_market_breadth(
    *,
    breadth: list[float] | None = None,
    breadth_ma: list[float] | None = None,
    latest_at: datetime | None = None,
) -> MarketBreadthSeries:
    breadth_values = breadth if breadth is not None else HIGHER_LOW_BREADTH
    breadth_ma_values = breadth_ma if breadth_ma is not None else breadth_values
    count = len(breadth_values)
    latest_timestamp = latest_at or NOW - timedelta(minutes=5)
    return MarketBreadthSeries(
        timestamp=[
            (latest_timestamp - timedelta(minutes=15 * offset)).isoformat()
            for offset in reversed(range(count))
        ],
        advancers=[500] * count,
        decliners=[500] * count,
        market_breadth=breadth_values,
        market_breadth_ma=breadth_ma_values,
        avg_gain=[0.03] * count,
        avg_loss=[-0.01] * count,
        total_volume=[1_000.0] * count,
        strength_index=[0.1] * count,
    )


def make_btc_df(*, downtrend: bool = True) -> pd.DataFrame:
    closes = (
        [120.0 - index for index in range(20)]
        if downtrend
        else [100.0 + index for index in range(20)]
    )
    return pd.DataFrame({"close": closes})


def make_lower_high_df(*, fresh: bool = True) -> pd.DataFrame:
    highs = [100.0 + index for index in range(30)]
    highs.extend([140.0, 132.0, 124.0, 120.0, 125.0, 130.0, 133.0, 136.0, 130.0, 124.0])
    lows = [high - 2 for high in highs]
    lows[20] = 115.0
    closes = [high - 1 for high in highs]
    open_times = [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(len(highs))]
    frame = pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": closes,
            "open_time": open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in open_times],
        }
    )
    if fresh:
        return frame

    return pd.concat(
        [
            frame,
            pd.DataFrame(
                {
                    "high": [118.0],
                    "low": [116.0],
                    "close": [117.0],
                    "open_time": [BASE_OPEN_TIME_MS + len(highs) * BAR_MS],
                    "close_time": [BASE_OPEN_TIME_MS + (len(highs) + 1) * BAR_MS - 1],
                }
            ),
        ],
        ignore_index=True,
    )


def make_no_lower_high_df() -> pd.DataFrame:
    highs = [100.0 + index for index in range(40)]
    return pd.DataFrame(
        {
            "high": highs,
            "low": [high - 2 for high in highs],
            "close": [high - 1 for high in highs],
            "open_time": [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(40)],
            "close_time": [
                BASE_OPEN_TIME_MS + (index + 1) * BAR_MS - 1 for index in range(40)
            ],
        }
    )


def make_top_gainers(
    *,
    symbol_rank: int | None = 4,
    recorded_at: datetime | None = None,
    snapshot_count: int = 7,
    price_change_percent: float = 18.5,
) -> list[GainersLosersSnapshot]:
    latest_at = recorded_at or NOW - timedelta(minutes=5)
    snapshots = []
    for hour_offset in range(snapshot_count):
        entries = [
            GainerLoserEntry(
                symbol=f"COIN{rank}USDTM",
                price_change_percent=float(30 - rank),
            )
            for rank in range(1, 13)
        ]
        if symbol_rank is not None:
            entries[symbol_rank - 1] = GainerLoserEntry(
                symbol="TESTUSDTM",
                price_change_percent=price_change_percent,
            )
        snapshots.append(
            GainersLosersSnapshot(
                source="kucoin_futures",
                recorded_at=(latest_at - timedelta(hours=hour_offset)).isoformat(),
                top_gainers=entries,
                top_losers=[],
            )
        )
    return snapshots


def make_context(
    *,
    breadth: MarketBreadthSeries | None = None,
    btc_df: pd.DataFrame | None = None,
    symbol_df: pd.DataFrame | None = None,
    symbol_rank: int = 4,
    market_type: MarketType = MarketType.FUTURES,
    gainers: list[GainersLosersSnapshot] | None = None,
    environment: str = "production",
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env=environment),
        symbol="TESTUSDTM",
        exchange=ExchangeId.KUCOIN,
        market_type=market_type,
        current_symbol_data=SymbolModel(
            id="TESTUSDTM",
            exchange_id=ExchangeId.KUCOIN,
            base_asset="TEST",
            quote_asset="USDT",
            price_precision=4,
        ),
        price_precision=4,
        telegram_consumer=SimpleNamespace(dispatch_signal=AsyncMock()),
        at_consumer=SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=AsyncMock(),
        ),
        market_breadth_data=breadth or make_market_breadth(),
        df_btc_15m=btc_df if btc_df is not None else make_btc_df(),
        df_15m=symbol_df if symbol_df is not None else make_lower_high_df(),
        df_1d=None,
        gainers_losers_series=(
            gainers
            if gainers is not None
            else make_top_gainers(symbol_rank=symbol_rank)
        ),
        strategy_cooldowns={},
        latest_market_context=None,
        regime_telegram_lines=Mock(return_value="- Regime measures: test"),
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
    )


@pytest.fixture(autouse=True)
def fixed_strategy_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "strategies.top_gainer_short.time",
        lambda: NOW.timestamp(),
    )


def test_strategy_requires_six_hour_watch_and_confirmed_breadth_higher_low() -> None:
    strategy = TopGainerShort(cast(Any, make_context()))

    watch = strategy._top_gainer_watch()
    breadth = strategy._breadth_higher_low()

    assert watch is not None
    assert watch["top_gainer_watch_hours"] == 6.0
    assert breadth is not None
    assert breadth["breadth_higher_low_first_trough"] == -0.5
    assert breadth["breadth_higher_low_second_trough"] == -0.3
    assert (
        breadth["breadth_higher_low_confirmation_timestamp"]
        == (NOW - timedelta(minutes=5)).timestamp()
    )


@pytest.mark.asyncio
async def test_signal_emits_protected_short_for_complete_bearish_setup() -> None:
    context = make_context()

    await TopGainerShort(cast(Any, context)).signal(
        current_price=135.0,
        bb_high=95.0,
        bb_mid=92.0,
        bb_low=87.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.autotrade is False
    assert value.direction == "SHORT"
    assert value.bot_params.name == "top_gainer_short"
    assert value.bot_params.position == "short"
    assert value.bot_params.stop_loss == 0.9926
    assert value.bot_params.dynamic_trailing is False
    assert value.bot_params.trailing is True
    assert value.bot_params.trailing_profit == 4.5
    assert value.bot_params.trailing_deviation == 3.0
    assert value.bot_params.margin_short_reversal is False
    assert value.bot_params.recovery_params is None
    assert "recovery_params" in value.bot_params.model_fields_set
    assert indicators["entry_reason"] == "lower_high_breakdown"
    assert indicators["top_gainer_watch_hours"] == 6.0
    assert indicators["strong_gainer"] is False
    assert indicators["breadth_higher_low_confirmed"] is True
    assert indicators["breadth_higher_low_first_trough"] == -0.5
    assert indicators["breadth_higher_low_second_trough"] == -0.3
    assert not any(key.startswith("btc_") for key in indicators)
    assert "breadth_falling_three_hours" not in indicators
    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Breadth higher low confirmed: -0.5 -> -0.3" in msg
    assert "BTC" not in msg
    assert "Breadth falling" not in msg
    assert indicators["lower_high_first_peak"] == 140.0
    assert indicators["lower_high_second_peak"] == 136.0
    assert indicators["price_crossings_six_hours"] == 2
    assert not any(key.startswith("weekly_") for key in indicators)
    assert indicators["stop_loss_source"] == "lower_high"
    assert indicators["stop_loss_price_at_signal"] == 136.34
    assert indicators["protective_exit"] == "exchange_native_reduce_only_stop"
    assert value.score == 1.0
    context.regime_telegram_lines.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_signal_requests_autotrade_in_staging_only() -> None:
    staging = make_context(environment="staging")
    production = make_context(environment="production")
    development = make_context(environment="development")

    for context in (staging, production, development):
        await TopGainerShort(cast(Any, context)).signal(
            current_price=135.0,
            bb_high=95.0,
            bb_mid=92.0,
            bb_low=87.0,
        )

    staging_signal = staging.dispatch_signal_record.await_args.kwargs["value"]
    production_signal = production.dispatch_signal_record.await_args.kwargs["value"]
    development_signal = development.dispatch_signal_record.await_args.kwargs["value"]
    assert staging_signal.autotrade is True
    assert production_signal.autotrade is False
    assert development_signal.autotrade is False
    assert (
        "Autotrade: enabled for staging"
        in (staging.telegram_consumer.dispatch_signal.call_args.args[0])
    )
    assert (
        "Autotrade: disabled; notification only"
        in (production.telegram_consumer.dispatch_signal.call_args.args[0])
    )


@pytest.mark.asyncio
async def test_signal_uses_lower_high_stop_without_hourly_candles() -> None:
    context = make_context()

    await TopGainerShort(cast(Any, context)).signal(
        current_price=135.0,
        bb_high=92.0,
        bb_mid=90.0,
        bb_low=88.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.bot_params.stop_loss == 0.9926
    assert indicators["stop_loss_source"] == "lower_high"
    assert indicators["stop_loss_price_at_signal"] == 136.34

    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Stop loss: 0.25% above lower high at 136.34 (0.9926%)" in msg
    assert "Trailing stop: arms after 4.5% profit with 3.0% deviation" in msg
    assert "Autotrade: disabled; notification only" in msg


@pytest.mark.asyncio
async def test_signal_requires_fresh_confirmed_lower_high() -> None:
    contexts = [
        make_context(symbol_df=make_no_lower_high_df()),
        make_context(
            symbol_df=make_lower_high_df(fresh=False).assign(
                open_time=lambda df: df.open_time - BAR_MS,
                close_time=lambda df: df.close_time - BAR_MS,
            )
        ),
    ]

    for context in contexts:
        await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)
        context.dispatch_signal_record.assert_not_awaited()
        context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_uses_latest_completed_candle_for_lower_high() -> None:
    frame = make_lower_high_df()
    live_open_time = int(frame["open_time"].iloc[-1]) + BAR_MS
    frame = pd.concat(
        [
            frame,
            pd.DataFrame(
                {
                    "high": [138.0],
                    "low": [122.0],
                    "close": [137.0],
                    "open_time": [live_open_time],
                    "close_time": [int(NOW.timestamp() * 1000) + BAR_MS],
                }
            ),
        ],
        ignore_index=True,
    )
    context = make_context(symbol_df=frame)

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["lower_high_confirmation_open_time"] == int(
        frame["open_time"].iloc[-2]
    )


@pytest.mark.asyncio
async def test_signal_rejects_stale_gainers_snapshot() -> None:
    context = make_context(
        gainers=make_top_gainers(recorded_at=NOW - timedelta(minutes=76))
    )

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("btc_state", ["falling", "rising", "missing"])
async def test_btc_does_not_change_signal_or_score(btc_state: str) -> None:
    context = make_context(btc_df=make_btc_df(downtrend=btc_state == "falling"))
    if btc_state == "missing":
        del context.df_btc_15m

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    assert value.score == 1.0


@pytest.mark.parametrize("offset", [0.0, 0.5, 0.6])
def test_breadth_higher_low_handles_negative_zero_and_positive_troughs(offset):
    context = make_context(
        breadth=make_market_breadth(breadth=[v + offset for v in HIGHER_LOW_BREADTH])
    )

    pattern = TopGainerShort(cast(Any, context))._breadth_higher_low()

    assert pattern is not None
    assert pattern["breadth_higher_low_rise"] == pytest.approx(0.2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "stale",
        "future",
        "short",
        "monotonic",
        "equal_lows",
        "lower_low",
        "unconfirmed",
        "breached",
        "breached_then_recovered",
        "nan",
        "infinity",
        "gap",
        "duplicates",
        "misaligned",
        "invalid_timestamp",
        "newer_lower_low",
    ],
)
async def test_signal_blocks_without_valid_breadth_higher_low(case: str) -> None:
    breadth = make_market_breadth()
    values = list(HIGHER_LOW_BREADTH)
    if case == "stale":
        breadth = make_market_breadth(latest_at=NOW - timedelta(minutes=31))
    elif case == "future":
        breadth = make_market_breadth(latest_at=NOW + timedelta(minutes=1))
    elif case == "short":
        breadth = make_market_breadth(breadth=values[1:])
    elif case == "monotonic":
        breadth.market_breadth = [i / 100 for i in range(40)]
    elif case == "equal_lows":
        breadth.market_breadth[37] = -0.5
    elif case == "lower_low":
        breadth.market_breadth[37] = -0.6
    elif case == "unconfirmed":
        breadth.market_breadth[-3:] = [-0.1, -0.3, -0.1]
    elif case == "breached":
        breadth = make_market_breadth(breadth=values + [-0.4])
    elif case == "breached_then_recovered":
        breadth = make_market_breadth(breadth=values + [-0.4, 0.0])
    elif case == "newer_lower_low":
        breadth = make_market_breadth(breadth=values + [-0.4, -0.2, 0.0])
    elif case in {"nan", "infinity"}:
        breadth.market_breadth[-1] = float("nan" if case == "nan" else "inf")
    elif case == "gap":
        breadth.timestamp[0] = (NOW - timedelta(hours=12)).isoformat()
    elif case == "duplicates":
        breadth.timestamp[-1] = breadth.timestamp[-2]
    elif case == "misaligned":
        breadth.timestamp.pop()
    elif case == "invalid_timestamp":
        breadth.timestamp[-1] = "invalid"
    context = make_context(breadth=breadth)
    if case == "missing":
        context.market_breadth_data = None

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.telegram_consumer.dispatch_signal.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


def test_breadth_pattern_remains_valid_after_confirmation_until_breached():
    breadth = make_market_breadth(breadth=HIGHER_LOW_BREADTH + [0.1, 0.2])
    # API order is irrelevant as long as timestamp/value pairs remain intact.
    breadth.timestamp.reverse()
    breadth.market_breadth.reverse()
    pattern = TopGainerShort(
        cast(Any, make_context(breadth=breadth))
    )._breadth_higher_low()

    assert pattern is not None
    assert (
        pattern["breadth_higher_low_confirmation_timestamp"]
        == (NOW - timedelta(minutes=35)).timestamp()
    )
    assert pattern["breadth_latest"] == 0.2


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol_rank", [1, 10])
async def test_signal_accepts_any_rank_in_top_gainers_snapshot(
    symbol_rank: int,
) -> None:
    context = make_context(symbol_rank=symbol_rank)

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["top_gainer_rank"] == symbol_rank


@pytest.mark.asyncio
async def test_signal_requires_current_top_gainer_membership() -> None:
    context = make_context(gainers=make_top_gainers(symbol_rank=None))

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gainers",
    [
        make_top_gainers(snapshot_count=6),
        make_top_gainers(symbol_rank=11),
    ],
)
async def test_signal_requires_a_six_hour_top_ten_watch(
    gainers: list[GainersLosersSnapshot],
) -> None:
    context = make_context(gainers=gainers)

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_emits_only_once_for_same_lower_high_confirmation() -> None:
    context = make_context()
    strategy = TopGainerShort(cast(Any, context))

    for _ in range(2):
        await strategy.signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_awaited_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_signal_tags_a_twenty_percent_gainer_as_high_priority() -> None:
    context = make_context(
        gainers=make_top_gainers(price_change_percent=23.0),
    )

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["strong_gainer"] is True
    assert indicators["top_gainer_watch_max_gain_24h_pct"] == 23.0
    assert value.score == 1.5


@pytest.mark.asyncio
async def test_signal_ignores_non_futures_market() -> None:
    context = make_context(market_type=MarketType.SPOT)

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_marks_emitted_even_when_autotrade_processing_raises() -> None:
    """The signal record may already be persisted by the time a later
    fallible step raises; the confirmation must still be marked emitted so
    the next tick doesn't see it as new and reprocess/duplicate it."""
    context = make_context()
    context.at_consumer.process_autotrade_restrictions = AsyncMock(
        side_effect=RuntimeError("boom")
    )
    strategy = TopGainerShort(cast(Any, context))

    with pytest.raises(RuntimeError):
        await strategy.signal(135.0, 95.0, 92.0, 87.0)

    await strategy.signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_awaited_once()


@pytest.mark.parametrize(
    ("closes", "expected"),
    [
        ([134.0] * 24, 0),
        ([134.0] * 12 + [136.0] * 12, 1),
        ([134.0] * 8 + [136.0] * 8 + [134.0] * 8, 2),
        ([134.0] * 6 + [136.0] * 6 + [134.0] * 6 + [136.0] * 6, 3),
        ([134.0, 136.0] * 12, 23),
        ([134.0, 135.0, 134.0, 135.0, 136.0, 135.0] * 4, 7),
        ([135.0] * 24, 0),
    ],
)
def test_crossings_count_side_changes_and_ignore_exact_touches(closes, expected):
    frame = make_lower_high_df()
    frame.loc[frame.index[-24:], "close"] = closes
    # Wicks straddle the reference on every bar; they are not close crossings.
    frame.loc[:, "high"] = 140.0
    frame.loc[:, "low"] = 130.0
    assert (
        price_crossings_six_hours(frame, 135.0, now_ms=NOW.timestamp() * 1000)
        == expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("crossings", [2, 3])
async def test_chop_gate_blocks_at_three_crossings(monkeypatch, crossings):
    context = make_context()
    strategy = TopGainerShort(cast(Any, context))
    monkeypatch.setattr(
        "strategies.top_gainer_short.price_crossings_six_hours",
        lambda *args, **kwargs: crossings,
    )

    await strategy.signal(135.0, 140.0, 132.0, 120.0)

    if crossings == 2:
        context.dispatch_signal_record.assert_awaited_once()
    else:
        context.dispatch_signal_record.assert_not_awaited()
        context.telegram_consumer.dispatch_signal.assert_not_awaited()
        context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
        assert context.strategy_cooldowns == {}


@pytest.mark.asyncio
async def test_repeated_crossings_reject_a_real_confirmed_lower_high():
    context = make_context()
    strategy = TopGainerShort(cast(Any, context))
    assert strategy._fresh_lower_high() is not None
    assert (
        price_crossings_six_hours(context.df_15m, 124.0, now_ms=NOW.timestamp() * 1000)
        == 4
    )

    await strategy.signal(124.0, 140.0, 132.0, 120.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.telegram_consumer.dispatch_signal.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.parametrize(
    "invalid_history", ["short", "gap", "duplicate", "stale", "nan", "infinite", "zero"]
)
def test_chop_check_requires_complete_recent_valid_history(invalid_history):
    frame = make_lower_high_df()
    if invalid_history == "short":
        frame = frame.tail(23)
    elif invalid_history == "gap":
        frame = frame.drop(frame.index[-10])
    elif invalid_history == "duplicate":
        frame = pd.concat([frame, frame.tail(1)], ignore_index=True)
    elif invalid_history == "stale":
        frame["open_time"] -= BAR_MS
        frame["close_time"] -= BAR_MS
    else:
        frame.loc[frame.index[-10], "close"] = {
            "nan": float("nan"),
            "infinite": float("inf"),
            "zero": 0.0,
        }[invalid_history]
    assert (
        price_crossings_six_hours(frame, 135.0, now_ms=NOW.timestamp() * 1000) is None
    )


def test_chop_check_ignores_old_and_forming_candles():
    frame = make_lower_high_df()
    frame.loc[frame.index[:-24], "close"] = [134.0, 136.0] * 8
    frame.loc[frame.index[-24:], "close"] = 134.0
    live_open = int(NOW.timestamp() * 1000) // BAR_MS * BAR_MS
    frame = pd.concat(
        [
            frame,
            pd.DataFrame(
                {
                    "open_time": [live_open],
                    "close_time": [live_open + BAR_MS - 1],
                    "high": [137.0],
                    "low": [133.0],
                    "close": [136.0],
                }
            ),
        ],
        ignore_index=True,
    )
    assert price_crossings_six_hours(frame, 135.0, now_ms=NOW.timestamp() * 1000) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "current_price", [136.0, 136.1, 0.0, float("nan"), float("inf")]
)
async def test_signal_rejects_reclaimed_lower_high_or_invalid_price(current_price):
    context = make_context()

    await TopGainerShort(cast(Any, context)).signal(current_price, 140.0, 132.0, 120.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_rounded_stop_stays_above_lower_high():
    context = make_context()
    context.price_precision = 0

    await TopGainerShort(cast(Any, context)).signal(135.0, 140.0, 132.0, 120.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["stop_loss_price_at_signal"] == 137.0
    assert indicators["stop_loss_pct"] == 1.4815

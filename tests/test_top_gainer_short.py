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

from strategies.top_gainer_short import TopGainerShort

NOW = datetime(2026, 9, 23, 10, 20, tzinfo=UTC)
BEARISH_CROSS_BREADTH = [0.30] * 10 + [0.24, 0.20, 0.16]
BEARISH_CROSS_BREADTH_MA = [0.24] * 10 + [0.235, 0.225, 0.21]
BULLISH_CROSS_BREADTH = [-0.30] * 10 + [-0.24, -0.20, -0.16]
BULLISH_CROSS_BREADTH_MA = [-0.24] * 10 + [-0.235, -0.225, -0.21]

BASE_OPEN_TIME_MS = 1_700_000_000_000
BAR_MS = 15 * 60 * 1000


def make_market_breadth(
    *,
    breadth: list[float] | None = None,
    breadth_ma: list[float] | None = None,
    latest_at: datetime | None = None,
) -> MarketBreadthSeries:
    breadth_values = breadth or BEARISH_CROSS_BREADTH
    breadth_ma_values = breadth_ma or BEARISH_CROSS_BREADTH_MA
    latest_timestamp = latest_at or NOW - timedelta(minutes=5)
    return MarketBreadthSeries(
        timestamp=[
            (latest_timestamp - timedelta(minutes=15 * offset)).isoformat()
            for offset in reversed(range(13))
        ],
        advancers=[500] * 13,
        decliners=[500] * 13,
        market_breadth=breadth_values,
        market_breadth_ma=breadth_ma_values,
        avg_gain=[0.03] * 13,
        avg_loss=[-0.01] * 13,
        total_volume=[1_000.0] * 13,
        strength_index=[0.1] * 13,
    )


def make_btc_df(*, downtrend: bool = True) -> pd.DataFrame:
    closes = (
        [120.0 - index for index in range(20)]
        if downtrend
        else [100.0 + index for index in range(20)]
    )
    return pd.DataFrame({"close": closes})


def make_weekly_structure_df() -> pd.DataFrame:
    candle_count = 7 * 24
    first_open_time = int((NOW - timedelta(hours=candle_count + 1)).timestamp() * 1000)
    highs = [95.0] * candle_count
    lows = [85.0] * candle_count
    highs[0] = 96.0
    lows[1] = 84.0
    open_times = [
        first_open_time + index * 60 * 60 * 1000 for index in range(candle_count)
    ]
    return pd.DataFrame(
        {
            "high": highs,
            "low": lows,
            "close": [90.0] * candle_count,
            "open_time": open_times,
            "close_time": [open_time + 60 * 60 * 1000 - 1 for open_time in open_times],
        }
    )


def make_daily_df(
    *,
    prior_high: float = 80.0,
    prior_open: float = 40.0,
    prior_close: float = 40.0,
    prior_candles: int = 60,
) -> pd.DataFrame:
    """Daily candles that all closed before the 7-day recent window."""
    day_ms = 24 * 60 * 60 * 1000
    first_open_time = int((NOW - timedelta(days=prior_candles + 8)).timestamp() * 1000)
    open_times = [first_open_time + index * day_ms for index in range(prior_candles)]
    return pd.DataFrame(
        {
            "open": [prior_open] * prior_candles,
            "high": [prior_high] * prior_candles,
            "low": [prior_open] * prior_candles,
            "close": [prior_close] * prior_candles,
            "open_time": open_times,
            "close_time": [open_time + day_ms - 1 for open_time in open_times],
        }
    )


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
    weekly_df: pd.DataFrame | None = None,
    daily_df: pd.DataFrame | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="production"),
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
        df_1h=weekly_df if weekly_df is not None else make_weekly_structure_df(),
        df_1d=daily_df if daily_df is not None else pd.DataFrame(),
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


def test_strategy_uses_six_hour_watch_and_three_hour_macro_windows() -> None:
    strategy = TopGainerShort(cast(Any, make_context()))

    watch = strategy._top_gainer_watch()
    breadth = strategy._breadth_falling_three_hours()
    btc = strategy._btc_falling_three_hours()

    assert watch is not None
    assert watch["top_gainer_watch_hours"] == 6.0
    assert breadth is not None
    assert breadth["breadth_change_three_hours"] < 0
    assert btc is not None
    assert btc["btc_change_three_hours_pct"] < 0


@pytest.mark.asyncio
async def test_signal_emits_protected_short_for_complete_bearish_setup() -> None:
    context = make_context()

    await TopGainerShort(cast(Any, context)).signal(
        current_price=90.0,
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
    assert value.bot_params.stop_loss == 6.9333
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
    assert indicators["breadth_falling_three_hours"] is True
    assert indicators["btc_falling_three_hours"] is True
    assert indicators["btc_close_15m"] == pytest.approx(101.0)
    assert indicators["lower_high_first_peak"] == 140.0
    assert indicators["lower_high_second_peak"] == 136.0
    assert indicators["weekly_resistance"] == 96.0
    assert indicators["weekly_support"] == 84.0
    assert indicators["weekly_structure_candles"] == 168
    assert indicators["stop_loss_source"] == "weekly_resistance"
    assert indicators["stop_loss_price_at_signal"] == 96.24
    assert indicators["protective_exit"] == "exchange_native_reduce_only_stop"
    assert value.score == 2.0
    context.regime_telegram_lines.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_signal_uses_weekly_resistance_independent_of_bollinger_band() -> None:
    context = make_context()

    await TopGainerShort(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=92.0,
        bb_mid=90.0,
        bb_low=88.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.bot_params.stop_loss == 6.9333
    assert indicators["stop_loss_source"] == "weekly_resistance"
    assert indicators["stop_loss_price_at_signal"] == 96.24

    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Stop loss: 0.25% above weekly resistance at 96.24 (6.9333%)" in msg
    assert "Trailing stop: arms after 4.5% profit with 3.0% deviation" in msg
    assert "Autotrade is disabled; notification only" in msg


@pytest.mark.asyncio
async def test_signal_requires_fresh_confirmed_lower_high() -> None:
    contexts = [
        make_context(symbol_df=make_no_lower_high_df()),
        make_context(symbol_df=make_lower_high_df(fresh=False)),
    ]

    for context in contexts:
        await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)
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

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["lower_high_confirmation_open_time"] == int(
        frame["open_time"].iloc[-2]
    )


@pytest.mark.asyncio
async def test_signal_rejects_stale_gainers_snapshot() -> None:
    context = make_context(
        gainers=make_top_gainers(recorded_at=NOW - timedelta(minutes=76))
    )

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_still_enters_without_btc_falling_confirmation() -> None:
    """BTC deterioration changes priority but does not replace the coin's
    lower-high failure as the entry trigger."""
    context = make_context(btc_df=make_btc_df(downtrend=False))

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["btc_falling_three_hours"] is False
    assert indicators["breadth_falling_three_hours"] is True
    assert value.score == 1.5


@pytest.mark.asyncio
async def test_signal_still_enters_without_breadth_falling_confirmation() -> None:
    """Breadth deterioration changes priority but does not replace the coin's
    lower-high failure as the entry trigger."""
    context = make_context(
        breadth=make_market_breadth(
            breadth=BULLISH_CROSS_BREADTH,
            breadth_ma=BULLISH_CROSS_BREADTH_MA,
        )
    )

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["breadth_falling_three_hours"] is False
    assert indicators["btc_falling_three_hours"] is True
    assert value.score == 1.5


@pytest.mark.asyncio
async def test_signal_treats_stale_breadth_as_unconfirmed() -> None:
    """A market-breadth refresh failure makes KlinesProvider retain the
    previous snapshot; a historical cross buried in that stale data must
    not be treated as a live confirmation (no score bonus, no indicators)."""
    context = make_context(
        breadth=make_market_breadth(latest_at=NOW - timedelta(minutes=31))
    )

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["breadth_falling_three_hours"] is False
    assert "breadth_latest" not in indicators
    assert indicators["btc_falling_three_hours"] is True
    assert value.score == 1.5


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol_rank", [1, 10])
async def test_signal_accepts_any_rank_in_top_gainers_snapshot(
    symbol_rank: int,
) -> None:
    context = make_context(symbol_rank=symbol_rank)

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["top_gainer_rank"] == symbol_rank


@pytest.mark.asyncio
async def test_signal_requires_current_top_gainer_membership() -> None:
    context = make_context(gainers=make_top_gainers(symbol_rank=None))

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

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

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_requires_seven_days_of_completed_hourly_candles() -> None:
    context = make_context(weekly_df=make_weekly_structure_df().iloc[:-1])

    await TopGainerShort(cast(Any, context)).signal(90.0, 89.0, 88.0, 85.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_emits_only_once_for_same_breadth_cross() -> None:
    context = make_context()
    strategy = TopGainerShort(cast(Any, context))

    for _ in range(2):
        await strategy.signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_awaited_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_signal_tags_a_twenty_percent_gainer_as_high_priority() -> None:
    context = make_context(
        gainers=make_top_gainers(price_change_percent=23.0),
    )

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["strong_gainer"] is True
    assert indicators["top_gainer_watch_max_gain_24h_pct"] == 23.0
    assert value.score == 2.5


@pytest.mark.asyncio
async def test_signal_ignores_non_futures_market() -> None:
    context = make_context(market_type=MarketType.SPOT)

    await TopGainerShort(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

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
        await strategy.signal(90.0, 95.0, 92.0, 87.0)

    await strategy.signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_awaited_once()


@pytest.mark.asyncio
async def test_stretched_move_raises_score_and_reports_breakdown() -> None:
    """Weekly high 96 is above every prior daily high (80) and 140% above the
    prior median close (40); the 18.5% gain is below the prior best daily
    gain (100%), so only two of three stretch bonuses apply."""
    context = make_context(daily_df=make_daily_df())

    await TopGainerShort(cast(Any, context)).signal(
        current_price=90.0, bb_high=95.0, bb_mid=92.0, bb_low=87.0
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    message = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert value.score == 3.0
    assert indicators["stretch_score"] == 1.0
    assert indicators["stretch_record_gain"] is False
    assert indicators["stretch_new_high"] is True
    assert indicators["stretch_extended"] is True
    assert "Stretch score: +1.0 of 1.5" in message
    assert "Confidence score: 3.0" in message


@pytest.mark.asyncio
async def test_move_inside_prior_daily_range_adds_no_stretch_score() -> None:
    context = make_context(
        daily_df=make_daily_df(prior_high=120.0, prior_open=60.0, prior_close=90.0)
    )

    await TopGainerShort(cast(Any, context)).signal(
        current_price=90.0, bb_high=95.0, bb_mid=92.0, bb_low=87.0
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["stretch_score"] == 0.0
    assert value.score == 2.0


@pytest.mark.asyncio
async def test_short_daily_history_gives_no_stretch_score_and_does_not_gate() -> None:
    context = make_context(daily_df=make_daily_df(prior_candles=5))

    await TopGainerShort(cast(Any, context)).signal(
        current_price=90.0, bb_high=95.0, bb_mid=92.0, bb_low=87.0
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    message = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert value.score == 2.0
    assert indicators["stretch_score"] == 0.0
    assert "Stretch score: n/a" in message

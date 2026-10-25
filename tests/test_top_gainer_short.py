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
    KucoinKlineIntervals,
    MarketBreadthSeries,
    MarketType,
    SymbolModel,
)

from pybinbot.shared.price_crossings import (
    CHOP_CROSSING_SCORE_PENALTY,
    CHOP_CROSSING_THRESHOLD,
    price_crossings_six_hours,
)

from market_regime.models import DerivativesPositioningFeatures
from market_regime.open_interest_order_sizing import apply_open_interest_sizing
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


def make_btc_higher_low_df(lows: list[float] | None = None) -> pd.DataFrame:
    values = (
        lows if lows is not None else [100 + value * 10 for value in HIGHER_LOW_BREADTH]
    )
    end = int(NOW.timestamp() * 1000) // BAR_MS * BAR_MS
    open_times = [end - (len(values) - i) * BAR_MS for i in range(len(values))]
    return pd.DataFrame(
        {
            "open_time": open_times,
            "close_time": [timestamp + BAR_MS - 1 for timestamp in open_times],
            "low": values,
            "high": [value + 2 for value in values],
            "close": [value + 1 for value in values],
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
    price_change_percent: float = 23.0,
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
    if symbol_df is None:
        symbol_df = make_lower_high_df()
        # Keep both fractal peaks above a one-sided six-hour candle history.
        # The preceding swing low sits outside that gate's lookback window.
        symbol_df["high"] = symbol_df["high"].clip(lower=135.2)
        symbol_df["close"] = symbol_df["close"].clip(lower=135.1)
        symbol_df["low"] = symbol_df["low"].clip(lower=135.05)
        symbol_df.loc[10, "low"] = 105.0

    return SimpleNamespace(
        config=SimpleNamespace(
            env=environment, frontend_domain="https://terminal.example.com/"
        ),
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
        candlestick_interval=KucoinKlineIntervals.FIFTEEN_MINUTES,
        market_breadth_data=breadth or make_market_breadth(),
        df_btc_15m=btc_df if btc_df is not None else make_btc_df(),
        df_15m=symbol_df,
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


def test_strategy_tracks_watch_duration_and_confirmed_breadth_higher_low() -> None:
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
    assert value.autotrade is True
    assert value.direction == "SHORT"
    assert value.bot_params.name == "top_gainer_short"
    assert value.bot_params.position == "short"
    assert value.bot_params.stop_loss == 2.7556
    assert value.bot_params.dynamic_trailing is False
    assert value.bot_params.dynamic_position_sizing is True
    assert value.bot_params.take_profit == 4.5
    assert value.bot_params.trailing is True
    assert value.bot_params.trailing_profit == 4.5
    assert value.bot_params.trailing_deviation == 0.3704
    assert value.bot_params.recovery_params is None
    assert value.bot_params.recovery_params is None
    assert "recovery_params" in value.bot_params.model_fields_set
    assert indicators["entry_reason"] == "lower_high_breakdown"
    assert indicators["top_gainer_watch_hours"] == 6.0
    assert indicators["current_top_gainer"] is True
    assert indicators["top_gainer_score_bonus"] == 2.0
    assert indicators["strong_gainer"] is True
    assert indicators["breadth_higher_low_confirmed"] is True
    assert indicators["breadth_higher_low_first_trough"] == -0.5
    assert indicators["breadth_higher_low_second_trough"] == -0.3
    assert indicators["btc_higher_low_confirmed"] is False
    assert "btc_higher_low_first_trough" not in indicators
    assert "breadth_falling_three_hours" not in indicators
    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "Breadth higher low confirmed: -0.5 -> -0.3" in msg
    assert "BTC higher low confirmed: No" in msg
    assert "Breadth falling" not in msg
    assert indicators["lower_high_first_peak"] == 140.0
    assert indicators["lower_high_second_peak"] == 136.0
    assert indicators["price_crossings_six_hours"] == 0
    assert not any(key.startswith("weekly_") for key in indicators)
    assert indicators["stop_loss_source"] == "lower_high"
    assert indicators["lower_high_stop_buffer_pct"] == 2.0
    assert indicators["stop_loss_price_at_signal"] == 138.72
    assert indicators["protective_exit"] == "dynamic_position_sizing"
    assert "default 25% reduction" in msg
    assert "increase at 4.5% profit" in msg
    assert "dynamic sizing replaces full stop" in msg
    assert value.score == 3.5
    context.regime_telegram_lines.assert_called_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,oi_change,original_margin,expected_margin",
    [
        ("NEW_LEVERAGE_SHORT", 0.02, 8.0, 8.0),
        ("NEW_LEVERAGE_SHORT", 0.005, 6.0, 6.0),
        ("NEW_LEVERAGE_LONG", 0.005, 3.0, 3.0),
        ("NEW_LEVERAGE_LONG", 0.02, 2.0, 2.0),
        (None, None, 4.0, 4.0),
    ],
)
async def test_peak_based_stop_preserves_open_interest_margin_without_risk_clamp(
    state, oi_change, original_margin, expected_margin
):
    context = make_context(environment="staging")
    positioning = (
        DerivativesPositioningFeatures(
            timestamp=1_000_000,
            open_interest=1000.0,
            open_interest_notional=135000.0,
            positioning_state=state,
            oi_change_15m=oi_change,
        )
        if state is not None
        else None
    )

    def finalize(value):
        value.open_interest_sizing = apply_open_interest_sizing(
            bot_params=value.bot_params,
            positioning=positioning,
            signal_timestamp=1_000_000,
        )

    context.finalize_signal_bot_params = Mock(side_effect=finalize)
    await TopGainerShort(cast(Any, context)).signal(135.0, 140.0, 132.0, 120.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    params = value.bot_params
    assert params.fiat_order_size == expected_margin
    assert params.stop_loss == 2.7556
    assert params.dynamic_position_sizing is True
    assert params.take_profit == 4.5
    assert (
        "stop_risk_sizing"
        not in context.dispatch_signal_record.await_args.kwargs["indicators"]
    )
    if value.open_interest_sizing is not None:
        # OI evidence retains its intermediate sizing decision.
        assert value.open_interest_sizing.adjusted_margin == original_margin
    msg = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert f"Initial margin: {expected_margin} USDT" in msg
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "environment,autotrade_enabled",
    [
        ("staging", True),
        ("production", True),
        ("PRODUCTION", True),
        ("development", False),
    ],
)
async def test_signal_requests_autotrade_in_staging_and_production(
    environment: str, autotrade_enabled: bool
) -> None:
    context = make_context(environment=environment)

    await TopGainerShort(cast(Any, context)).signal(
        current_price=135.0,
        bb_high=95.0,
        bb_mid=92.0,
        bb_low=87.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    assert value.autotrade is autotrade_enabled
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)
    message = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert (
        "<a href='https://terminal.example.com/bots/futures/new/TESTUSDTM'>Dashboard trade</a>"
        in message
    )
    expected_status = "enabled" if autotrade_enabled else "disabled; notification only"
    assert f"Autotrade: {expected_status}" in message


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
    assert value.bot_params.stop_loss == 2.7556
    assert indicators["stop_loss_source"] == "lower_high"
    assert indicators["stop_loss_price_at_signal"] == 138.72

    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert (
        "Initial reduction threshold: 2.0% above lower high at 138.72 (2.7556%)" in msg
    )
    assert "Trailing fallback: arms after 4.5% profit with 0.3704% deviation" in msg
    assert "Autotrade: enabled" in msg


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
    frame = make_context().df_15m.copy()
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
@pytest.mark.parametrize(
    "gainers",
    [
        pytest.param([], id="missing"),
        pytest.param(make_top_gainers(symbol_rank=None), id="unranked"),
        pytest.param(make_top_gainers(symbol_rank=11), id="rank-eleven"),
        pytest.param(make_top_gainers(symbol_rank=12), id="rank-twelve"),
        pytest.param(
            make_top_gainers(
                recorded_at=NOW - timedelta(minutes=76), price_change_percent=23.0
            ),
            id="stale",
        ),
        pytest.param(
            make_top_gainers(recorded_at=NOW + timedelta(minutes=1)), id="future"
        ),
    ],
)
async def test_signal_emits_without_top_gainer_bonus_when_ranking_is_unavailable(
    gainers: list[GainersLosersSnapshot],
) -> None:
    context = make_context(gainers=gainers)

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_awaited_once()
    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.score == 1.0
    assert indicators["current_top_gainer"] is False
    assert indicators["top_gainer_score_bonus"] == 0.0
    assert indicators["strong_gainer"] is False
    assert "top_gainer_rank" not in indicators
    msg = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert "Top-gainer context: no fresh top-10 membership" in msg
    assert "Top-gainer score bonus: +0.0" in msg
    assert "SHORT when price confirms a lower high" in msg
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("btc_state", ["falling", "rising", "missing"])
async def test_breadth_confirmation_passes_without_btc_higher_low_or_score_change(
    btc_state: str,
) -> None:
    context = make_context(btc_df=make_btc_df(downtrend=btc_state == "falling"))
    if btc_state == "missing":
        context.df_btc_15m = pd.DataFrame()

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    assert value.score == 3.5


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
async def test_current_top_gainer_scores_considerably_above_same_unranked_setup() -> (
    None
):
    ranked = make_context()
    unranked = make_context(gainers=make_top_gainers(symbol_rank=None))

    for context in (ranked, unranked):
        await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    ranked_signal = ranked.dispatch_signal_record.await_args.kwargs["value"]
    unranked_signal = unranked.dispatch_signal_record.await_args.kwargs["value"]
    assert ranked_signal.score - unranked_signal.score == 2.5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot_count",
    [1, 6, 7],
)
async def test_top_gainer_bonus_does_not_require_six_hour_watch(
    snapshot_count: int,
) -> None:
    context = make_context(gainers=make_top_gainers(snapshot_count=snapshot_count))

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.score == 3.5
    assert indicators["top_gainer_watch_hours"] == snapshot_count - 1
    assert indicators["top_gainer_score_bonus"] == 2.0
    msg = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert "Top-gainer score bonus: +2.0" in msg


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
    assert value.score == 3.5


@pytest.mark.asyncio
@pytest.mark.parametrize("ranked", [False, True])
async def test_stretch_scores_price_history_without_inventing_missing_gainer_data(
    ranked: bool,
) -> None:
    context = make_context(
        gainers=make_top_gainers(price_change_percent=23.0) if ranked else []
    )
    context.df_1d = pd.DataFrame(
        {
            "open": [40.0] * 14,
            "high": [44.0] * 14,
            "close": [40.0] * 14,
            "close_time": [
                (NOW - timedelta(days=offset)).timestamp() * 1000
                for offset in range(8, 22)
            ],
        }
    )

    await TopGainerShort(cast(Any, context)).signal(135.0, 95.0, 92.0, 87.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    # New high and extension still score without a ranking; record gain needs data.
    assert value.score == (5.0 if ranked else 2.0)
    msg = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert f"Stretch score: +{1.5 if ranked else 1.0}" in msg
    assert "weekly high above all prior daily highs: Yes" in msg
    assert (
        "record 24h gain: Yes (23.0%"
        if ranked
        else "record 24h gain: n/a (no fresh top-10 gain data)"
    ) in msg


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
    frame["high"] = frame["close"]
    frame["low"] = frame["close"]
    assert (
        price_crossings_six_hours(
            frame, 135.0, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS
        )
        == expected
    )


@pytest.mark.parametrize(
    ("current_price", "prices", "expected_crossings"),
    [
        pytest.param(
            0.004248,
            [
                (0.004484, 0.004462, 0.004479),
                (0.004485, 0.00446, 0.00446),
                (0.004478, 0.004445, 0.004456),
                (0.004488, 0.00446, 0.004475),
                (0.004464, 0.004447, 0.004447),
                (0.004453, 0.004421, 0.004451),
                (0.004452, 0.004413, 0.004452),
                (0.004453, 0.004416, 0.004442),
                (0.004434, 0.004416, 0.004425),
                (0.004425, 0.004407, 0.004407),
                (0.004406, 0.004363, 0.004404),
                (0.004395, 0.004379, 0.004395),
                (0.004408, 0.004397, 0.004398),
                (0.004408, 0.004388, 0.004388),
                (0.004425, 0.004386, 0.004425),
                (0.004455, 0.004408, 0.004408),
                (0.004577, 0.004315, 0.004362),
                (0.004385, 0.004342, 0.004344),
                (0.004357, 0.004329, 0.004345),
                (0.004347, 0.004329, 0.00433),
                (0.00434, 0.004328, 0.004338),
                (0.004362, 0.004338, 0.004338),
                (0.004345, 0.004324, 0.004325),
                (0.004335, 0.00397, 0.00427),
            ],
            1,
            id="SOLVUSDTM-signal-195830",
        ),
        pytest.param(
            0.06515,
            [
                (0.06494, 0.06339, 0.06356),
                (0.06411, 0.06365, 0.06387),
                (0.06415, 0.06378, 0.0639),
                (0.06377, 0.06255, 0.06349),
                (0.06424, 0.06349, 0.06413),
                (0.06442, 0.06373, 0.06391),
                (0.0654, 0.06372, 0.0652),
                (0.06766, 0.06535, 0.06742),
                (0.06774, 0.06643, 0.06691),
                (0.06709, 0.06645, 0.0666),
                (0.06688, 0.06643, 0.06647),
                (0.06736, 0.06562, 0.06663),
                (0.06619, 0.06489, 0.06579),
                (0.06574, 0.06522, 0.06547),
                (0.06667, 0.06546, 0.06638),
                (0.06861, 0.06638, 0.06861),
                (0.06883, 0.06638, 0.06713),
                (0.06747, 0.06661, 0.06699),
                (0.06725, 0.06635, 0.06677),
                (0.06642, 0.06578, 0.06633),
                (0.06609, 0.06555, 0.06592),
                (0.06683, 0.06622, 0.06628),
                (0.06653, 0.06613, 0.06615),
                (0.06598, 0.06499, 0.06517),
            ],
            3,
            id="PHAUSDTM-signal-195828",
        ),
    ],
)
def test_staging_candle_ranges_include_wicks_at_entry(
    current_price, prices, expected_crossings
):
    # KuCoin's 24 completed 15m ranges and closes preceding the staging
    # signals on 2026-10-23. Rebase timestamps; preserve high, low, close.
    frame = make_lower_high_df().tail(24).copy()
    frame[["high", "low", "close"]] = prices
    moves = frame["close"].diff()
    assert (moves * moves.shift(1) < 0).sum() == 11

    crossings = price_crossings_six_hours(
        frame, current_price, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS
    )

    assert crossings is not None
    assert crossings == expected_crossings
    # PHA now rejects at three; SOLV's single excursion still passes.
    assert (crossings >= CHOP_CROSSING_THRESHOLD) == (current_price == 0.06515)


@pytest.mark.asyncio
@pytest.mark.parametrize("crossings", [0, 1, 2, 3, 4])
async def test_chop_gate_penalizes_crossings_and_rejects_at_three(
    monkeypatch, crossings
):
    context = make_context()
    strategy = TopGainerShort(cast(Any, context))
    monkeypatch.setattr(
        "strategies.top_gainer_short.price_crossings_six_hours",
        lambda *args, **kwargs: crossings,
    )

    await strategy.signal(135.0, 140.0, 132.0, 120.0)

    if crossings >= 3:
        context.dispatch_signal_record.assert_not_awaited()
        context.telegram_consumer.dispatch_signal.assert_not_awaited()
        context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
        assert context.strategy_cooldowns == {}
        return

    context.dispatch_signal_record.assert_awaited_once()
    context.telegram_consumer.dispatch_signal.assert_awaited_once()
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    value = context.dispatch_signal_record.await_args.kwargs["value"]
    expected_penalty = crossings * CHOP_CROSSING_SCORE_PENALTY
    assert indicators["price_crossings_six_hours"] == crossings
    assert indicators["chop_crossing_threshold"] == CHOP_CROSSING_THRESHOLD == 3
    assert indicators["chop_score_penalty"] == expected_penalty
    assert value.score == round(3.5 - expected_penalty, 4)
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_repeated_crossings_reject_a_real_confirmed_lower_high():
    context = make_context()
    context.df_15m = make_lower_high_df()
    strategy = TopGainerShort(cast(Any, context))
    assert strategy._fresh_lower_high() is not None
    assert (
        price_crossings_six_hours(
            context.df_15m, 124.0, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS
        )
        == 6
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
        price_crossings_six_hours(
            frame, 135.0, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS
        )
        is None
    )


def test_chop_check_ignores_old_and_forming_candles():
    frame = make_lower_high_df()
    frame.loc[frame.index[:-24], "close"] = [134.0, 136.0] * 8
    frame.loc[frame.index[-24:], "close"] = 134.0
    frame["high"] = frame["close"]
    frame["low"] = frame["close"]
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
    assert (
        price_crossings_six_hours(
            frame, 135.0, now_ms=NOW.timestamp() * 1000, interval_ms=BAR_MS
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "current_price", [136.0, 136.1, 0.0, float("nan"), float("inf")]
)
async def test_signal_rejects_reclaimed_lower_high_or_invalid_price(current_price):
    context = make_context()

    await TopGainerShort(cast(Any, context)).signal(current_price, 140.0, 132.0, 120.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("current_price", [69.36, 69.1])
async def test_signal_rejects_stop_distance_outside_dynamic_sizing_range(
    monkeypatch, current_price
):
    context = make_context()
    monkeypatch.setattr(
        "strategies.top_gainer_short.price_crossings_six_hours",
        lambda *args, **kwargs: 0,
    )

    await TopGainerShort(cast(Any, context)).signal(current_price, 140.0, 132.0, 120.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_rounded_stop_stays_above_lower_high():
    context = make_context()
    context.price_precision = 0

    await TopGainerShort(cast(Any, context)).signal(135.0, 140.0, 132.0, 120.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["stop_loss_price_at_signal"] == 139.0
    assert indicators["stop_loss_pct"] == 2.963


@pytest.mark.parametrize(
    "score,expected_leverage",
    [
        (0.8, 1),
        (1.0, 1),
        (1.5, 2),
        (2.0, 2),
        (2.5, 3),
        (3.0, 3),
        (3.5, 4),
        (4.0, 4),
        (4.5, 5),
        (5.0, 5),
        (-1, 1),
        (6, 5),
    ],
)
def test_score_maps_possible_range_to_whole_futures_leverage(score, expected_leverage):
    assert TopGainerShort.MIN_SCORE == pytest.approx(0.8)
    assert TopGainerShort.MAX_SCORE == 5.0
    assert TopGainerShort.futures_leverage_for_score(score) == expected_leverage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ranked,crossings,expected_score,expected_leverage",
    [
        (True, 0, 3.5, 4),
        (True, 1, 3.4, 3),
        (True, 2, 3.3, 3),
        (False, 2, 0.8, 1),
    ],
)
async def test_emitted_leverage_uses_final_score_after_crossing_penalty(
    monkeypatch,
    ranked,
    crossings,
    expected_score,
    expected_leverage,
):
    context = make_context(
        gainers=make_top_gainers(price_change_percent=23.0) if ranked else [],
    )
    monkeypatch.setattr(
        "strategies.top_gainer_short.price_crossings_six_hours",
        lambda *args, **kwargs: crossings,
    )

    await TopGainerShort(cast(Any, context)).signal(135.0, 140.0, 132.0, 120.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.score == expected_score
    assert value.bot_params.futures_leverage == expected_leverage
    assert indicators["futures_leverage"] == expected_leverage
    assert indicators["futures_leverage_score_min"] == 0.8
    assert indicators["futures_leverage_score_max"] == 5.0
    message = context.telegram_consumer.dispatch_signal.await_args.args[0]
    assert f"Futures leverage: {expected_leverage}x" in message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "breadth_confirmed,btc_confirmed",
    [(True, False), (False, True), (True, True), (False, False)],
)
async def test_signal_passes_if_either_btc_or_breadth_has_confirmed_higher_low(
    breadth_confirmed, btc_confirmed
):
    context = make_context(
        breadth=make_market_breadth()
        if breadth_confirmed
        else make_market_breadth(breadth=[i / 100 for i in range(40)]),
        btc_df=make_btc_higher_low_df() if btc_confirmed else pd.DataFrame(),
    )
    await TopGainerShort(cast(Any, context)).signal(135, 95, 92, 87)
    if not breadth_confirmed and not btc_confirmed:
        context.dispatch_signal_record.assert_not_awaited()
        context.telegram_consumer.dispatch_signal.assert_not_awaited()
        context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
        return
    context.dispatch_signal_record.assert_awaited_once()
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["breadth_higher_low_confirmed"] is breadth_confirmed
    assert indicators["btc_higher_low_confirmed"] is btc_confirmed
    assert context.dispatch_signal_record.await_args.kwargs["value"].score == 3.5
    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert "BTC or the breadth index has a confirmed higher low" in msg
    if btc_confirmed:
        assert indicators["btc_higher_low_first_trough"] == 95
        assert indicators["btc_higher_low_second_trough"] == 97
        assert "BTC higher low confirmed: 95.0 -> 97.0" in msg
    else:
        assert "btc_higher_low_first_trough" not in indicators
        assert "BTC higher low confirmed: No" in msg
    if not breadth_confirmed:
        assert "breadth_higher_low_first_trough" not in indicators
        assert "Breadth higher low confirmed: No" in msg


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["missing", "stale", "nan", "invalid_timestamps"])
async def test_valid_btc_higher_low_passes_when_breadth_is_unavailable_or_invalid(case):
    breadth = make_market_breadth()
    if case == "stale":
        breadth = make_market_breadth(latest_at=NOW - timedelta(minutes=31))
    elif case == "nan":
        breadth.market_breadth[-1] = float("nan")
    elif case == "invalid_timestamps":
        breadth.timestamp[-1] = "invalid"
    context = make_context(breadth=breadth, btc_df=make_btc_higher_low_df())
    if case == "missing":
        context.market_breadth_data = None
    await TopGainerShort(cast(Any, context)).signal(135, 95, 92, 87)
    context.dispatch_signal_record.assert_awaited_once()
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["btc_higher_low_confirmed"] is True
    assert indicators["breadth_higher_low_confirmed"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("breadth_confirmed", [False, True])
@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "stale",
        "insufficient",
        "gap",
        "duplicate",
        "nan",
        "inf",
        "nonpositive",
        "invalid_timestamps",
        "missing_columns",
        "lower_low",
        "equal_low",
        "breached",
        "unconfirmed",
    ],
)
async def test_invalid_btc_higher_low_does_not_confirm_and_cannot_veto_valid_breadth(
    case, breadth_confirmed
):
    values = [100 + value * 10 for value in HIGHER_LOW_BREADTH]
    frame = make_btc_higher_low_df()
    if case == "stale":
        frame["close_time"] -= 31 * 60 * 1000
    elif case == "insufficient":
        frame = frame.iloc[1:]
    elif case == "gap":
        frame = make_btc_higher_low_df([103] + values).drop(index=10)
    elif case == "duplicate":
        frame.loc[10, "close_time"] = frame.loc[9, "close_time"]
    elif case in {"nan", "inf", "nonpositive"}:
        frame.loc[10, "low"] = 0 if case == "nonpositive" else float(case)
    elif case == "invalid_timestamps":
        frame.loc[10, "close_time"] = float("nan")
    elif case == "missing_columns":
        frame = frame.drop(columns=["low"])
    elif case in {"lower_low", "equal_low"}:
        frame.loc[37, "low"] = 94 if case == "lower_low" else 95
    elif case == "breached":
        frame = make_btc_higher_low_df(values + [96])
    elif case == "unconfirmed":
        frame = make_btc_higher_low_df(values[:-1])
    context = make_context(
        btc_df=frame,
        breadth=make_market_breadth()
        if breadth_confirmed
        else make_market_breadth(breadth=[i / 100 for i in range(40)]),
    )
    if case == "missing":
        context.df_btc_15m = None
    strategy = TopGainerShort(cast(Any, context))
    assert strategy._btc_higher_low() is None
    await strategy.signal(135, 95, 92, 87)
    if breadth_confirmed:
        context.dispatch_signal_record.assert_awaited_once()
        indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
        assert indicators["breadth_higher_low_confirmed"] is True
        assert indicators["btc_higher_low_confirmed"] is False
    else:
        context.dispatch_signal_record.assert_not_awaited()


def test_btc_higher_low_uses_completed_candles_and_remains_valid_after_confirmation():
    values = [100 + value * 10 for value in HIGHER_LOW_BREADTH]
    frame = make_btc_higher_low_df(values + [101, 102])
    current_open = int(NOW.timestamp() * 1000) // BAR_MS * BAR_MS
    # A forming wick below the trough cannot invalidate completed structure.
    frame.loc[len(frame)] = [current_open, current_open + BAR_MS - 1, 90, 103, 91]
    frame = frame.iloc[::-1]
    strategy = TopGainerShort(cast(Any, make_context(btc_df=frame)))
    pattern = strategy._btc_higher_low()
    assert pattern is not None
    assert pattern["btc_higher_low_first_trough"] == 95
    assert pattern["btc_higher_low_second_trough"] == 97
    assert pattern["btc_latest"] == 102


@pytest.mark.asyncio
async def test_btc_higher_low_does_not_bypass_the_symbol_lower_high_trigger():
    context = make_context(
        btc_df=make_btc_higher_low_df(), symbol_df=make_no_lower_high_df()
    )
    await TopGainerShort(cast(Any, context)).signal(135, 95, 92, 87)
    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gain,qualifies", [(18.5, False), (20.0, False), (20.0001, True)]
)
async def test_top_gainer_bonus_requires_latest_gain_strictly_above_twenty_percent(
    gain, qualifies
):
    gainers = make_top_gainers(price_change_percent=30.0)
    gainers[0].top_gainers[3].price_change_percent = gain
    context = make_context(gainers=gainers)
    await TopGainerShort(cast(Any, context)).signal(135.0, 140.0, 132.0, 120.0)
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    value = context.dispatch_signal_record.await_args.kwargs["value"]
    assert indicators["current_top_gainer"] is qualifies
    assert indicators["top_gainer_score_bonus"] == (2.0 if qualifies else 0.0)
    assert value.score == (3.5 if qualifies else 1.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("peak,expected", [(136.0, 0.3704), (137.0, 0.7408)])
async def test_trailing_deviation_is_half_the_unbuffered_latest_swing_peak_distance(
    monkeypatch, peak, expected
):
    context = make_context()
    strategy = TopGainerShort(cast(Any, context))
    pattern = strategy._fresh_lower_high()
    assert pattern is not None
    pattern["later_high"] = peak
    monkeypatch.setattr(strategy, "_fresh_lower_high", lambda: pattern)
    await strategy.signal(135.0, 140.0, 132.0, 120.0)
    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.bot_params.trailing_deviation == expected
    assert indicators["trailing_deviation_pct"] == expected
    assert indicators["stop_loss_price_at_signal"] == pytest.approx(peak * 1.02)
    assert value.bot_params.stop_loss == indicators["stop_loss_pct"]
    assert (
        f"with {expected}% deviation"
        in context.telegram_consumer.dispatch_signal.await_args.args[0]
    )

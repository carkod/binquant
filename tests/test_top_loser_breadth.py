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
    breadth_momentum_reversal,
    btc_trend_confirms,
)

from strategies.top_loser_breadth import TopLoserBreadth

NOW = datetime(2026, 9, 23, 10, 20, tzinfo=UTC)
BULLISH_CROSS_BREADTH = [-0.30] * 9 + [-0.24, -0.20, -0.16]
BULLISH_CROSS_BREADTH_MA = [-0.24] * 9 + [-0.235, -0.225, -0.21]
BEARISH_CROSS_BREADTH = [0.30] * 9 + [0.24, 0.20, 0.16]
BEARISH_CROSS_BREADTH_MA = [0.24] * 9 + [0.235, 0.225, 0.21]
HIGH_FLOOR_BREADTH = [value - 0.45 for value in BULLISH_CROSS_BREADTH]
HIGH_FLOOR_BREADTH_MA = [value - 0.45 for value in BULLISH_CROSS_BREADTH_MA]

BASE_OPEN_TIME_MS = 1_700_000_000_000
BAR_MS = 15 * 60 * 1000


def make_market_breadth(
    *,
    breadth: list[float] | None = None,
    breadth_ma: list[float] | None = None,
    latest_at: datetime | None = None,
) -> MarketBreadthSeries:
    breadth_values = breadth or BULLISH_CROSS_BREADTH
    breadth_ma_values = breadth_ma or BULLISH_CROSS_BREADTH_MA
    latest_timestamp = latest_at or NOW - timedelta(minutes=5)
    return MarketBreadthSeries(
        timestamp=[
            (latest_timestamp - timedelta(minutes=15 * offset)).isoformat()
            for offset in reversed(range(12))
        ],
        advancers=[500] * 12,
        decliners=[500] * 12,
        market_breadth=breadth_values,
        market_breadth_ma=breadth_ma_values,
        avg_gain=[0.03] * 12,
        avg_loss=[-0.01] * 12,
        total_volume=[1_000.0] * 12,
        strength_index=[0.1] * 12,
    )


def make_btc_df(*, uptrend: bool = True) -> pd.DataFrame:
    closes = (
        [100.0 + index for index in range(20)]
        if uptrend
        else [120.0 - index for index in range(20)]
    )
    return pd.DataFrame({"close": closes})


def make_higher_low_df(*, fresh: bool = True) -> pd.DataFrame:
    lows = [140.0 - index for index in range(30)]
    lows.extend([100.0, 108.0, 116.0, 120.0, 115.0, 110.0, 107.0, 104.0, 110.0, 116.0])
    highs = [low + 2 for low in lows]
    highs[20] = 125.0
    closes = [low + 1 for low in lows]
    open_times = [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(len(lows))]
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
                    "high": [122.0],
                    "low": [120.0],
                    "close": [121.0],
                    "open_time": [BASE_OPEN_TIME_MS + len(lows) * BAR_MS],
                    "close_time": [BASE_OPEN_TIME_MS + (len(lows) + 1) * BAR_MS - 1],
                }
            ),
        ],
        ignore_index=True,
    )


def make_beta_ready_dfs(
    *, beta_ratio: float = 1.5, window_bars: int = TopLoserBreadth.BTC_BETA_WINDOW_BARS
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    A `df_15m`-shaped frame satisfying the higher-low pattern in its final
    40 bars (HigherLowPattern.LOOKBACK_BARS), with `window_bars` of extra
    synthetic history prepended where the symbol's return is exactly
    `beta_ratio` times BTC's return per bar - enough aligned history for
    latest_beta() to resolve to a known value - plus a matching-length BTC
    frame.
    """
    btc_return_cycle = [0.01, -0.02, 0.02, -0.01]
    token_return_cycle = [r * beta_ratio for r in btc_return_cycle]
    repeats = window_bars // len(btc_return_cycle) + 1

    btc_closes = [100.0]
    token_closes = [50.0]
    for _ in range(repeats):
        for btc_r, token_r in zip(btc_return_cycle, token_return_cycle, strict=True):
            btc_closes.append(btc_closes[-1] * (1 + btc_r))
            token_closes.append(token_closes[-1] * (1 + token_r))
    token_prefix = token_closes[:window_bars]

    symbol_tail = make_higher_low_df()
    prefix_open_times = [
        int(symbol_tail["open_time"].iloc[0]) - (window_bars - index) * BAR_MS
        for index in range(window_bars)
    ]
    symbol_prefix = pd.DataFrame(
        {
            "high": [close * 1.001 for close in token_prefix],
            "low": [close * 0.999 for close in token_prefix],
            "close": token_prefix,
            "open_time": prefix_open_times,
            "close_time": [open_time + BAR_MS - 1 for open_time in prefix_open_times],
        }
    )
    symbol_df = pd.concat([symbol_prefix, symbol_tail], ignore_index=True)
    btc_df = pd.DataFrame(
        {
            "close": btc_closes[:window_bars]
            + [btc_closes[window_bars - 1]] * len(symbol_tail)
        }
    )
    return symbol_df, btc_df


def make_no_higher_low_df() -> pd.DataFrame:
    lows = [140.0 - index for index in range(40)]
    return pd.DataFrame(
        {
            "high": [low + 2 for low in lows],
            "low": lows,
            "close": [low + 1 for low in lows],
            "open_time": [BASE_OPEN_TIME_MS + index * BAR_MS for index in range(40)],
            "close_time": [
                BASE_OPEN_TIME_MS + (index + 1) * BAR_MS - 1 for index in range(40)
            ],
        }
    )


def make_top_losers(
    *,
    symbol_rank: int = 4,
    recorded_at: datetime | None = None,
) -> list[GainersLosersSnapshot]:
    entries = [
        GainerLoserEntry(
            symbol=f"COIN{rank}USDTM",
            price_change_percent=float(rank - 30),
        )
        for rank in range(1, 13)
    ]
    entries[symbol_rank - 1] = GainerLoserEntry(
        symbol="TESTUSDTM",
        price_change_percent=-18.5,
    )
    return [
        GainersLosersSnapshot(
            source="kucoin_futures",
            recorded_at=(recorded_at or NOW - timedelta(minutes=5)).isoformat(),
            top_gainers=[],
            top_losers=entries,
        )
    ]


def make_context(
    *,
    breadth: MarketBreadthSeries | None = None,
    btc_df: pd.DataFrame | None = None,
    symbol_df: pd.DataFrame | None = None,
    symbol_rank: int = 4,
    market_type: MarketType = MarketType.FUTURES,
    losers: list[GainersLosersSnapshot] | None = None,
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
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(
            autotrade_settings=AutotradeSettingsSchema(
                fiat="USDT",
                base_order_size=6.0,
            ),
            process_autotrade_restrictions=AsyncMock(),
        ),
        market_breadth_data=breadth or make_market_breadth(),
        df_btc_15m=btc_df if btc_df is not None else make_btc_df(),
        df_15m=symbol_df if symbol_df is not None else make_higher_low_df(),
        gainers_losers_series=(
            losers if losers is not None else make_top_losers(symbol_rank=symbol_rank)
        ),
        strategy_cooldowns={},
        latest_market_context=None,
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
    )


@pytest.fixture(autouse=True)
def fixed_strategy_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "strategies.top_loser_breadth.time",
        lambda: NOW.timestamp(),
    )


def test_strategy_constants_classify_bullish_entry_fixture() -> None:
    breadth_values, breadth_reason = breadth_momentum_reversal(
        make_market_breadth(),
        direction=1,
        min_history=TopLoserBreadth.MIN_BREADTH_HISTORY,
        fast_ema_span=TopLoserBreadth.BREADTH_FAST_EMA_SPAN,
        extension_threshold=TopLoserBreadth.BREADTH_EXTENSION_THRESHOLD,
    )
    assert breadth_reason == "breadth_momentum_bullish_reversal"
    assert breadth_values is not None
    assert breadth_values["market_breadth"] == -0.16
    assert breadth_values["previous_breadth_oscillator"] < 0
    assert breadth_values["breadth_oscillator"] > 0

    btc_trend = btc_trend_confirms(
        make_btc_df(),
        direction=1,
        min_history=TopLoserBreadth.MIN_BTC_HISTORY,
        trend_ema_span=TopLoserBreadth.BTC_TREND_EMA_SPAN,
    )
    assert btc_trend is not None


@pytest.mark.asyncio
async def test_signal_emits_protected_long_for_complete_bullish_setup() -> None:
    context = make_context()

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=90.0,
        bb_low=85.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.autotrade is False
    assert value.direction == "LONG"
    assert value.bot_params.name == "top_loser_breadth"
    assert value.bot_params.position == "long"
    assert value.bot_params.stop_loss == 4.0
    assert value.bot_params.dynamic_trailing is True
    assert value.bot_params.trailing is True
    assert value.bot_params.trailing_profit == 3.5
    assert value.bot_params.trailing_deviation == 2.5
    assert value.bot_params.margin_short_reversal is False
    assert value.bot_params.recovery_params is None
    assert "recovery_params" in value.bot_params.model_fields_set
    assert indicators["entry_reason"] == "higher_low_breakout"
    assert indicators["breadth_reversal_confirmed"] is True
    assert indicators["breadth_reversal_reason"] == "breadth_momentum_bullish_reversal"
    assert indicators["market_breadth"] == -0.16
    assert indicators["btc_uptrend_confirmed"] is True
    assert indicators["btc_close_15m"] == pytest.approx(119.0)
    assert indicators["higher_low_first_trough"] == 100.0
    assert indicators["higher_low_second_trough"] == 104.0
    assert indicators["stop_loss_source"] == "max_stop_loss_cap"
    assert indicators["stop_loss_price_at_signal"] == 86.4
    assert indicators["protective_exit"] == "exchange_native_reduce_only_stop"
    assert value.score == 2.0
    # Default fixtures are far shorter than BTC_BETA_WINDOW_BARS: beta is
    # unavailable, not a bug, and must not block the signal.
    assert indicators["btc_beta"] is None
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_signal_reports_btc_beta_with_enough_history() -> None:
    symbol_df, btc_df = make_beta_ready_dfs(beta_ratio=1.5)
    context = make_context(symbol_df=symbol_df, btc_df=btc_df)

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    msg = context.telegram_consumer.dispatch_signal.call_args.args[0]
    assert indicators["btc_beta"] == pytest.approx(1.5, rel=1e-6)
    assert "Beta vs BTC" in msg
    assert "N/A" not in msg


@pytest.mark.asyncio
async def test_signal_uses_lower_bollinger_stop_when_inside_cap() -> None:
    context = make_context()

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=92.0,
        bb_mid=90.0,
        bb_low=88.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.bot_params.stop_loss == 2.2222
    assert indicators["stop_loss_source"] == "lower_bollinger_band"
    assert indicators["stop_loss_price_at_signal"] == 88.0


@pytest.mark.asyncio
async def test_signal_requires_fresh_confirmed_higher_low() -> None:
    contexts = [
        make_context(symbol_df=make_no_higher_low_df()),
        make_context(symbol_df=make_higher_low_df(fresh=False)),
    ]

    for context in contexts:
        await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)
        context.dispatch_signal_record.assert_not_awaited()
        context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_uses_latest_completed_candle_for_higher_low() -> None:
    frame = make_higher_low_df()
    live_open_time = int(frame["open_time"].iloc[-1]) + BAR_MS
    frame = pd.concat(
        [
            frame,
            pd.DataFrame(
                {
                    "high": [128.0],
                    "low": [114.0],
                    "close": [127.0],
                    "open_time": [live_open_time],
                    "close_time": [int(NOW.timestamp() * 1000) + BAR_MS],
                }
            ),
        ],
        ignore_index=True,
    )
    context = make_context(symbol_df=frame)

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["higher_low_confirmation_open_time"] == int(
        frame["open_time"].iloc[-2]
    )


@pytest.mark.asyncio
async def test_signal_rejects_stale_losers_snapshot() -> None:
    context = make_context(
        losers=make_top_losers(recorded_at=NOW - timedelta(minutes=76))
    )

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_still_enters_without_btc_uptrend_confirmation() -> None:
    """Breadth and BTC trend are confirming context, not entry gates: the
    higher-low price break is the trigger, so a missing BTC confirmation
    lowers the score but does not block entry."""
    context = make_context(btc_df=make_btc_df(uptrend=False))

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["btc_uptrend_confirmed"] is False
    assert "btc_close_15m" not in indicators
    assert indicators["breadth_reversal_confirmed"] is True
    assert value.score == 1.5


@pytest.mark.asyncio
async def test_signal_still_enters_without_breadth_reversal_confirmation() -> None:
    """Same as above for the breadth side: a stale or absent breadth
    reversal lowers the score but does not block a confirmed higher low."""
    context = make_context(
        breadth=make_market_breadth(
            breadth=BEARISH_CROSS_BREADTH,
            breadth_ma=BEARISH_CROSS_BREADTH_MA,
        )
    )

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["breadth_reversal_confirmed"] is False
    assert "market_breadth" not in indicators
    assert indicators["btc_uptrend_confirmed"] is True
    assert value.score == 1.5


@pytest.mark.asyncio
async def test_signal_treats_stale_breadth_as_unconfirmed() -> None:
    """A market-breadth refresh failure makes KlinesProvider retain the
    previous snapshot; a historical cross buried in that stale data must
    not be treated as a live confirmation (no score bonus, no indicators)."""
    context = make_context(
        breadth=make_market_breadth(latest_at=NOW - timedelta(minutes=31))
    )

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["breadth_reversal_confirmed"] is False
    assert "market_breadth" not in indicators
    assert indicators["btc_uptrend_confirmed"] is True
    assert value.score == 1.5


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol_rank", [1, 12])
async def test_signal_requires_second_through_eleventh_loser(symbol_rank: int) -> None:
    context = make_context(symbol_rank=symbol_rank)

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_accepts_eleventh_ranked_loser() -> None:
    context = make_context(symbol_rank=11)

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["top_loser_rank"] == 11


@pytest.mark.asyncio
async def test_signal_requires_lower_band_below_long_entry() -> None:
    context = make_context()

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 91.0, 91.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_emits_only_once_for_same_breadth_cross() -> None:
    context = make_context()
    strategy = TopLoserBreadth(cast(Any, context))

    for _ in range(2):
        await strategy.signal(90.0, 95.0, 90.0, 85.0)

    context.dispatch_signal_record.assert_awaited_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_signal_tags_high_conviction_breadth_floor() -> None:
    context = make_context(
        breadth=make_market_breadth(
            breadth=HIGH_FLOOR_BREADTH,
            breadth_ma=HIGH_FLOOR_BREADTH_MA,
        )
    )

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["market_breadth"] == pytest.approx(-0.61)
    assert indicators["breadth_floor"] == -0.6
    assert indicators["high_conviction_floor_reached"] is True


@pytest.mark.asyncio
async def test_signal_ignores_non_futures_market() -> None:
    context = make_context(market_type=MarketType.SPOT)

    await TopLoserBreadth(cast(Any, context)).signal(90.0, 95.0, 90.0, 85.0)

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
    strategy = TopLoserBreadth(cast(Any, context))

    with pytest.raises(RuntimeError):
        await strategy.signal(90.0, 95.0, 90.0, 85.0)

    await strategy.signal(90.0, 95.0, 90.0, 85.0)

    context.dispatch_signal_record.assert_awaited_once()

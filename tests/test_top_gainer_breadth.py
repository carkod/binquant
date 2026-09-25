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

from strategies.top_gainer_breadth import TopGainerBreadth

NOW = datetime(2026, 9, 23, 10, 20, tzinfo=UTC)
BEARISH_CROSS_BREADTH = [0.30] * 9 + [0.24, 0.20, 0.16]
BEARISH_CROSS_BREADTH_MA = [0.24] * 9 + [0.235, 0.225, 0.21]
BULLISH_CROSS_BREADTH = [-0.30] * 9 + [-0.24, -0.20, -0.16]
BULLISH_CROSS_BREADTH_MA = [-0.24] * 9 + [-0.235, -0.225, -0.21]
HIGH_CEILING_BREADTH = [value + 0.45 for value in BEARISH_CROSS_BREADTH]
HIGH_CEILING_BREADTH_MA = [value + 0.45 for value in BEARISH_CROSS_BREADTH_MA]

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
                BASE_OPEN_TIME_MS + (index + 1) * BAR_MS - 1
                for index in range(40)
            ],
        }
    )


def make_top_gainers(
    *,
    symbol_rank: int = 4,
    recorded_at: datetime | None = None,
) -> list[GainersLosersSnapshot]:
    entries = [
        GainerLoserEntry(
            symbol=f"COIN{rank}USDTM",
            price_change_percent=float(30 - rank),
        )
        for rank in range(1, 13)
    ]
    entries[symbol_rank - 1] = GainerLoserEntry(
        symbol="TESTUSDTM",
        price_change_percent=18.5,
    )
    return [
        GainersLosersSnapshot(
            source="kucoin_futures",
            recorded_at=(recorded_at or NOW - timedelta(minutes=5)).isoformat(),
            top_gainers=entries,
            top_losers=[],
        )
    ]


def make_context(
    *,
    breadth: MarketBreadthSeries | None = None,
    btc_df: pd.DataFrame | None = None,
    symbol_df: pd.DataFrame | None = None,
    symbol_rank: int = 4,
    market_type: MarketType = MarketType.FUTURES,
    gainers: list[GainersLosersSnapshot] | None = None,
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
        df_15m=symbol_df if symbol_df is not None else make_lower_high_df(),
        gainers_losers_series=(
            gainers
            if gainers is not None
            else make_top_gainers(symbol_rank=symbol_rank)
        ),
        strategy_cooldowns={},
        latest_market_context=None,
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
    )


@pytest.fixture(autouse=True)
def fixed_strategy_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "strategies.top_gainer_breadth.time",
        lambda: NOW.timestamp(),
    )


def test_strategy_constants_classify_bearish_entry_fixture() -> None:
    breadth_values, breadth_reason = breadth_momentum_reversal(
        make_market_breadth(),
        direction=-1,
        min_history=TopGainerBreadth.MIN_BREADTH_HISTORY,
        fast_ema_span=TopGainerBreadth.BREADTH_FAST_EMA_SPAN,
        extension_threshold=TopGainerBreadth.BREADTH_EXTENSION_THRESHOLD,
    )
    assert breadth_reason == "breadth_momentum_bearish_reversal"
    assert breadth_values is not None
    assert breadth_values["market_breadth"] == 0.16
    assert breadth_values["previous_breadth_oscillator"] > 0
    assert breadth_values["breadth_oscillator"] < 0

    btc_trend = btc_trend_confirms(
        make_btc_df(),
        direction=-1,
        min_history=TopGainerBreadth.MIN_BTC_HISTORY,
        trend_ema_span=TopGainerBreadth.BTC_TREND_EMA_SPAN,
    )
    assert btc_trend is not None


@pytest.mark.asyncio
async def test_signal_emits_protected_short_for_complete_bearish_setup() -> None:
    context = make_context()

    await TopGainerBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=92.0,
        bb_low=87.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.autotrade is True
    assert value.direction == "SHORT"
    assert value.bot_params.name == "top_gainer_breadth"
    assert value.bot_params.position == "short"
    assert value.bot_params.stop_loss == 4.0
    assert value.bot_params.dynamic_trailing is True
    assert value.bot_params.trailing is True
    assert value.bot_params.trailing_profit == 3.5
    assert value.bot_params.trailing_deviation == 2.5
    assert value.bot_params.margin_short_reversal is False
    assert value.bot_params.recovery_params is None
    assert "recovery_params" in value.bot_params.model_fields_set
    assert indicators["entry_reason"] == "breadth_momentum_bearish_reversal"
    assert indicators["market_breadth"] == 0.16
    assert indicators["btc_close_15m"] == pytest.approx(101.0)
    assert indicators["lower_high_first_peak"] == 140.0
    assert indicators["lower_high_second_peak"] == 136.0
    assert indicators["stop_loss_source"] == "max_stop_loss_cap"
    assert indicators["stop_loss_price_at_signal"] == 93.6
    assert indicators["protective_exit"] == "exchange_native_reduce_only_stop"
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_signal_uses_upper_bollinger_stop_when_inside_cap() -> None:
    context = make_context()

    await TopGainerBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=92.0,
        bb_mid=90.0,
        bb_low=88.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.bot_params.stop_loss == 2.2222
    assert indicators["stop_loss_source"] == "upper_bollinger_band"
    assert indicators["stop_loss_price_at_signal"] == 91.9999


@pytest.mark.asyncio
async def test_signal_requires_fresh_confirmed_lower_high() -> None:
    contexts = [
        make_context(symbol_df=make_no_lower_high_df()),
        make_context(symbol_df=make_lower_high_df(fresh=False)),
    ]

    for context in contexts:
        await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)
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

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["lower_high_confirmation_open_time"] == int(
        frame["open_time"].iloc[-2]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("breadth", "gainers"),
    [
        pytest.param(
            make_market_breadth(latest_at=NOW - timedelta(minutes=31)),
            None,
            id="stale-market-breadth",
        ),
        pytest.param(
            None,
            make_top_gainers(recorded_at=NOW - timedelta(minutes=76)),
            id="stale-gainers-snapshot",
        ),
    ],
)
async def test_signal_rejects_stale_market_tape(
    breadth: MarketBreadthSeries | None,
    gainers: list[GainersLosersSnapshot] | None,
) -> None:
    context = make_context(breadth=breadth, gainers=gainers)

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_requires_btc_downtrend() -> None:
    context = make_context(btc_df=make_btc_df(downtrend=False))

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_requires_bearish_breadth_cross() -> None:
    context = make_context(
        breadth=make_market_breadth(
            breadth=BULLISH_CROSS_BREADTH,
            breadth_ma=BULLISH_CROSS_BREADTH_MA,
        )
    )

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol_rank", [1, 12])
async def test_signal_requires_second_through_eleventh_gainer(symbol_rank: int) -> None:
    context = make_context(symbol_rank=symbol_rank)

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_accepts_eleventh_ranked_gainer() -> None:
    context = make_context(symbol_rank=11)

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["top_gainer_rank"] == 11


@pytest.mark.asyncio
async def test_signal_requires_upper_band_above_short_entry() -> None:
    context = make_context()

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 89.0, 88.0, 85.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_emits_only_once_for_same_breadth_cross() -> None:
    context = make_context()
    strategy = TopGainerBreadth(cast(Any, context))

    for _ in range(2):
        await strategy.signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_awaited_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_signal_tags_high_conviction_breadth_ceiling() -> None:
    context = make_context(
        breadth=make_market_breadth(
            breadth=HIGH_CEILING_BREADTH,
            breadth_ma=HIGH_CEILING_BREADTH_MA,
        )
    )

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["market_breadth"] == pytest.approx(0.61)
    assert indicators["breadth_ceiling"] == 0.6
    assert indicators["high_conviction_ceiling_reached"] is True


@pytest.mark.asyncio
async def test_signal_ignores_non_futures_market() -> None:
    context = make_context(market_type=MarketType.SPOT)

    await TopGainerBreadth(cast(Any, context)).signal(90.0, 95.0, 92.0, 87.0)

    context.dispatch_signal_record.assert_not_awaited()

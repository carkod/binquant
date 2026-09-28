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

# 15-minute market-breadth bars: 9 bars of extended bullish breadth (0.30),
# then a 3-bar fade ending at 0.16 (still >= 0.15, i.e. still extended) on
# the exact bar where the fast/slow EMA oscillator turns bearish. This is
# the entry pattern the strategy is designed to catch.
BREADTH_TIMESTAMPS = [
    "2026-09-23T07:30:00+00:00",
    "2026-09-23T07:45:00+00:00",
    "2026-09-23T08:00:00+00:00",
    "2026-09-23T08:15:00+00:00",
    "2026-09-23T08:30:00+00:00",
    "2026-09-23T08:45:00+00:00",
    "2026-09-23T09:00:00+00:00",
    "2026-09-23T09:15:00+00:00",
    "2026-09-23T09:30:00+00:00",
    "2026-09-23T09:45:00+00:00",
    "2026-09-23T10:00:00+00:00",
    "2026-09-23T10:15:00+00:00",
]
DEFAULT_BREADTH = [0.30] * 9 + [0.24, 0.20, 0.16]
DEFAULT_BREADTH_MA = [0.24] * 9 + [0.235, 0.225, 0.21]

# Exact sign-mirror of the entry pattern: 9 bars of extended bearish breadth
# (-0.30), then a 3-bar recovery ending at -0.16 (still extended <= -0.15) on
# the bar the oscillator turns bullish. This is the exit pattern.
EXIT_BREADTH = [-0.30] * 9 + [-0.24, -0.20, -0.16]
EXIT_BREADTH_MA = [-0.24] * 9 + [-0.235, -0.225, -0.21]


def make_market_breadth(
    *,
    breadth: list[float] | None = None,
    breadth_ma: list[float] | None = None,
) -> MarketBreadthSeries:
    breadth_values = breadth if breadth is not None else DEFAULT_BREADTH
    breadth_ma_values = breadth_ma if breadth_ma is not None else DEFAULT_BREADTH_MA
    length = len(breadth_values)
    return MarketBreadthSeries(
        timestamp=BREADTH_TIMESTAMPS[-length:],
        advancers=[500] * length,
        decliners=[500] * length,
        market_breadth=breadth_values,
        market_breadth_ma=breadth_ma_values,
        avg_gain=[0.03] * length,
        avg_loss=[-0.01] * length,
        total_volume=[1_000.0] * length,
        strength_index=[0.1] * length,
    )


def make_btc_df(*, uptrend: bool = True, rows: int = 20) -> pd.DataFrame:
    closes = (
        [100.0 + i for i in range(rows)]
        if uptrend
        else [100.0 - i for i in range(rows)]
    )
    return pd.DataFrame({"close": closes})


def make_binbot_api(*, active_bots: list[Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        get_bots_by_name=Mock(
            return_value=active_bots if active_bots is not None else []
        ),
        deactivate_bot=Mock(),
        submit_bot_event_logs=Mock(),
    )


def make_top_losers(*, symbol_rank: int = 4) -> list[GainersLosersSnapshot]:
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
            recorded_at="2026-09-23T10:15:00+00:00",
            top_gainers=[],
            top_losers=entries,
        )
    ]


def make_context(
    *,
    breadth: MarketBreadthSeries | None = None,
    btc_df: pd.DataFrame | None = None,
    binbot_api: SimpleNamespace | None = None,
    symbol_rank: int = 4,
) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(env="production"),
        symbol="TESTUSDTM",
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.FUTURES,
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
        binbot_api=binbot_api if binbot_api is not None else make_binbot_api(),
        market_breadth_data=breadth or make_market_breadth(),
        df_btc_15m=btc_df if btc_df is not None else make_btc_df(uptrend=False),
        gainers_losers_series=make_top_losers(symbol_rank=symbol_rank),
        strategy_cooldowns={},
        latest_market_context=None,
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
    )


def test_strategy_constants_classify_entry_and_exit_fixtures() -> None:
    """The oscillator/extension/trend math itself lives in pybinbot (and is
    covered by pybinbot's own test suite: tests/test_breadth.py). This just
    confirms TopLoserBreadth's specific constants (history/span/threshold)
    correctly classify this strategy's entry and exit fixtures."""
    entry_values, entry_reason = breadth_momentum_reversal(
        make_market_breadth(),
        direction=-1,
        min_history=TopLoserBreadth.MIN_BREADTH_HISTORY,
        fast_ema_span=TopLoserBreadth.BREADTH_FAST_EMA_SPAN,
        extension_threshold=TopLoserBreadth.BREADTH_EXTENSION_THRESHOLD,
    )
    assert entry_reason == "breadth_momentum_bearish_reversal"
    assert entry_values is not None
    assert entry_values["market_breadth"] == 0.16

    exit_values, exit_reason = breadth_momentum_reversal(
        make_market_breadth(breadth=EXIT_BREADTH, breadth_ma=EXIT_BREADTH_MA),
        direction=1,
        min_history=TopLoserBreadth.MIN_BREADTH_HISTORY,
        fast_ema_span=TopLoserBreadth.BREADTH_FAST_EMA_SPAN,
        extension_threshold=TopLoserBreadth.BREADTH_EXTENSION_THRESHOLD,
    )
    assert exit_reason == "breadth_momentum_bullish_reversal"
    assert exit_values is not None
    assert exit_values["market_breadth"] == -0.16

    entry_trend = btc_trend_confirms(
        make_btc_df(uptrend=False),
        direction=-1,
        min_history=TopLoserBreadth.MIN_BTC_HISTORY,
        trend_ema_span=TopLoserBreadth.BTC_TREND_EMA_SPAN,
    )
    assert entry_trend is not None

    exit_trend = btc_trend_confirms(
        make_btc_df(uptrend=True),
        direction=1,
        min_history=TopLoserBreadth.MIN_BTC_HISTORY,
        trend_ema_span=TopLoserBreadth.BTC_TREND_EMA_SPAN,
    )
    assert exit_trend is not None


@pytest.mark.asyncio
async def test_signal_emits_short_with_upper_bollinger_stop_and_trailing() -> None:
    context = make_context()

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=88.0,
        bb_low=89.0,
    )

    value = context.dispatch_signal_record.await_args.kwargs["value"]
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert value.autotrade is True
    assert value.direction == "SHORT"
    assert value.bot_params.name == "top_loser_breadth"
    assert value.bot_params.position == "short"
    assert value.bot_params.stop_loss == 5.5555
    assert value.bot_params.dynamic_trailing is True
    assert value.bot_params.trailing is True
    assert value.bot_params.trailing_profit == 6.0
    assert value.bot_params.trailing_deviation == 2.5
    assert value.bot_params.margin_short_reversal is True
    assert indicators["top_loser_rank"] == 4
    assert indicators["entry_reason"] == "breadth_momentum_bearish_reversal"
    assert indicators["market_breadth"] == 0.16
    assert indicators["btc_close_15m"] == pytest.approx(81.0)
    assert indicators["stop_loss_source"] == "upper_bollinger_band"
    assert indicators["stop_loss_price_at_signal"] == 95.0
    assert indicators["stop_loss_reversal_position"] == "long"
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(value)


@pytest.mark.asyncio
async def test_signal_excludes_the_very_top_loser() -> None:
    context = make_context(symbol_rank=1)

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=99.0,
        bb_mid=94.0,
        bb_low=89.0,
    )

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert context.strategy_cooldowns == {}


@pytest.mark.asyncio
async def test_signal_requires_symbol_within_ranked_loser_window() -> None:
    context = make_context(symbol_rank=12)

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=99.0,
        bb_mid=94.0,
        bb_low=89.0,
    )

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert context.strategy_cooldowns == {}


@pytest.mark.asyncio
async def test_signal_accepts_eleventh_ranked_loser() -> None:
    context = make_context(symbol_rank=11)

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=88.0,
        bb_low=89.0,
    )

    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["top_loser_rank"] == 11


@pytest.mark.asyncio
async def test_signal_requires_btc_downtrend() -> None:
    context = make_context(btc_df=make_btc_df(uptrend=True))

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=88.0,
        bb_low=89.0,
    )

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert context.strategy_cooldowns == {}


@pytest.mark.asyncio
async def test_signal_requires_upper_band_above_short_entry() -> None:
    context = make_context()

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=89.0,
        bb_mid=89.0,
        bb_low=87.0,
    )

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert context.strategy_cooldowns == {}


@pytest.mark.asyncio
async def test_signal_emits_only_once_for_same_breadth_cross() -> None:
    context = make_context()
    strategy = TopLoserBreadth(cast(Any, context))

    for _ in range(2):
        await strategy.signal(
            current_price=90.0,
            bb_high=95.0,
            bb_mid=88.0,
            bb_low=89.0,
        )

    context.dispatch_signal_record.assert_awaited_once()
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once()


@pytest.mark.asyncio
async def test_signal_closes_active_bot_on_bullish_reversal_with_btc_uptrend() -> None:
    active_bot = SimpleNamespace(id="bot-123")
    context = make_context(
        breadth=make_market_breadth(breadth=EXIT_BREADTH, breadth_ma=EXIT_BREADTH_MA),
        btc_df=make_btc_df(uptrend=True),
        binbot_api=make_binbot_api(active_bots=[active_bot]),
        symbol_rank=11,  # no longer a top loser; exit must not depend on this
    )

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=88.0,
        bb_low=89.0,
    )

    context.binbot_api.get_bots_by_name.assert_called_once_with(
        "top_loser_breadth", "TESTUSDTM"
    )
    context.binbot_api.deactivate_bot.assert_called_once_with(
        "bot-123", algorithmic_close=True
    )
    context.telegram_consumer.dispatch_signal.assert_called_once()
    # No entry side-effects: this bar is a bullish setup, not the bearish one.
    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()


@pytest.mark.asyncio
async def test_signal_exit_is_noop_when_no_active_bot() -> None:
    context = make_context(
        breadth=make_market_breadth(breadth=EXIT_BREADTH, breadth_ma=EXIT_BREADTH_MA),
        btc_df=make_btc_df(uptrend=True),
    )

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=88.0,
        bb_low=89.0,
    )

    context.binbot_api.get_bots_by_name.assert_called_once()
    context.binbot_api.deactivate_bot.assert_not_called()


@pytest.mark.asyncio
async def test_signal_exit_requires_btc_uptrend() -> None:
    context = make_context(
        breadth=make_market_breadth(breadth=EXIT_BREADTH, breadth_ma=EXIT_BREADTH_MA),
        btc_df=make_btc_df(uptrend=False),
        binbot_api=make_binbot_api(active_bots=[SimpleNamespace(id="bot-123")]),
    )

    await TopLoserBreadth(cast(Any, context)).signal(
        current_price=90.0,
        bb_high=95.0,
        bb_mid=88.0,
        bb_low=89.0,
    )

    context.binbot_api.get_bots_by_name.assert_not_called()
    context.binbot_api.deactivate_bot.assert_not_called()


@pytest.mark.asyncio
async def test_signal_deactivates_active_bot_only_once_for_same_breadth_cross() -> None:
    context = make_context(
        breadth=make_market_breadth(breadth=EXIT_BREADTH, breadth_ma=EXIT_BREADTH_MA),
        btc_df=make_btc_df(uptrend=True),
        binbot_api=make_binbot_api(active_bots=[SimpleNamespace(id="bot-123")]),
    )
    strategy = TopLoserBreadth(cast(Any, context))

    for _ in range(2):
        await strategy.signal(
            current_price=90.0,
            bb_high=95.0,
            bb_mid=88.0,
            bb_low=89.0,
        )

    context.binbot_api.get_bots_by_name.assert_called_once()
    context.binbot_api.deactivate_bot.assert_called_once()

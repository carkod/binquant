from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from pandas import DataFrame
from pybinbot import (
    BotModel,
    DealType,
    ExchangeId,
    MarketType,
    OrderStatus,
    Status,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from strategies.top_gainer_momentum_recovery import TopGainerMomentumRecovery


INTERVAL_MS = 15 * 60 * 1000
SYMBOL = "TESTUSDTM"


def make_recovery_candles() -> tuple[DataFrame, DataFrame, int]:
    now_ms = int(datetime.now(UTC).timestamp()) * 1000
    first_open_time = now_ms - 61 * INTERVAL_MS
    rows: list[dict[str, float | int]] = []
    for index in range(56):
        open_time = first_open_time + index * INTERVAL_MS
        rows.append(
            {
                "open_time": open_time,
                "close_time": open_time + INTERVAL_MS - 1,
                "open": 100.0,
                "high": 100.5,
                "low": 99.5,
                "close": 100.0,
                "volume": 100.0,
                "ATR": 2.0,
            }
        )

    source_position = len(rows) - 1
    rows[source_position].update(
        {
            "open": 100.0,
            "high": 111.0,
            "low": 99.8,
            "close": 110.0,
            "volume": 220.0,
        }
    )
    followup_rows = [
        (110.0, 115.0, 109.0, 114.0, 200.0),
        (114.0, 114.0, 106.0, 106.0, 150.0),
        (105.0, 108.0, 104.0, 107.5, 140.0),
        (107.5, 112.0, 107.0, 111.0, 180.0),
    ]
    for open_, high, low, close, volume in followup_rows:
        open_time = first_open_time + len(rows) * INTERVAL_MS
        rows.append(
            {
                "open_time": open_time,
                "close_time": open_time + INTERVAL_MS - 1,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "ATR": 2.0,
            }
        )

    candles = DataFrame(rows)
    btc_candles = candles.copy()
    btc_candles[["open", "high", "low", "close"]] = 100.0
    source_created_at = int(candles.iloc[source_position]["close_time"]) + 1
    return candles, btc_candles, source_created_at


def make_source_bot(
    *,
    status: Status,
    created_at: int,
    bot_id: int = 1,
    close_deal_type: DealType = DealType.stop_loss,
    open_exposure: bool = False,
) -> BotModel:
    orders = []
    deal = {
        "opening_price": 110.0 if status == Status.completed or open_exposure else 0,
        "opening_qty": 5.0 if status == Status.completed or open_exposure else 0,
        "opening_timestamp": created_at,
        "closing_price": 106.0 if status == Status.completed else 0,
        "closing_qty": 5.0 if status == Status.completed else 0,
        "closing_timestamp": created_at + 2 * INTERVAL_MS
        if status == Status.completed
        else 0,
    }
    if status == Status.completed:
        orders.append(
            {
                "order_type": "market",
                "time_in_force": "GTC",
                "timestamp": created_at + 2 * INTERVAL_MS,
                "order_id": f"order-{bot_id}",
                "order_side": "sell",
                "pair": SYMBOL,
                "qty": 5.0,
                "status": OrderStatus.FILLED,
                "price": 106.0,
                "deal_type": close_deal_type,
            }
        )
    return BotModel(
        id=UUID(int=bot_id),
        pair=SYMBOL,
        signal_id=42,
        name=TopGainerMomentumRecovery.SOURCE_ALGO,
        status=status,
        market_type=MarketType.FUTURES,
        created_at=created_at / 1000,
        updated_at=created_at / 1000,
        deal=deal,
        orders=orders,
    )


def make_symbol_features() -> SymbolMarketFeatures:
    return SymbolMarketFeatures(
        symbol=SYMBOL,
        timestamp=int(datetime.now(UTC).timestamp() * 1000),
        close=111.0,
        return_pct=0.03,
        ema20=108.0,
        ema50=105.0,
        above_ema20=True,
        above_ema50=True,
        trend_score=0.04,
        relative_strength_vs_btc=0.03,
        return_pct_horizon=0.10,
        relative_strength_vs_btc_horizon=0.08,
        atr_pct=0.02,
        bb_width=0.05,
        micro_regime="TREND_UP",
        micro_regime_strength=0.8,
        micro_regime_transition="BREAKOUT_UP",
        micro_regime_transition_strength=0.4,
    )


def make_market_context() -> LiveMarketContext:
    return LiveMarketContext(
        timestamp=int(datetime.now(UTC).timestamp() * 1000),
        market_stress_score=0.1,
        advancers_ratio=0.6,
        decliners_ratio=0.4,
        advancers=30,
        decliners=20,
        advancers_decliners_ratio=1.5,
        btc_present=True,
        fresh_count=50,
        total_tracked_symbols=50,
        coverage_ratio=1.0,
        btc_symbol="XBTUSDTM",
        confidence=1.0,
        is_provisional=False,
        average_return=0.01,
        average_relative_strength_vs_btc=0.01,
        pct_above_ema20=0.6,
        pct_above_ema50=0.55,
        average_trend_score=0.02,
        average_atr_pct=0.02,
        average_bb_width=0.04,
        btc_return=0.0,
        btc_trend_score=0.0,
        btc_regime_score=0.0,
        long_tailwind=0.2,
        short_tailwind=0.1,
        market_regime="RANGE",
        previous_market_regime="RANGE",
        market_regime_transition=None,
        market_regime_transition_strength=0.0,
        long_regime_score=0.3,
        short_regime_score=0.1,
        range_regime_score=0.7,
        stress_regime_score=0.1,
        regime_is_transitioning=False,
        symbol_features={SYMBOL: make_symbol_features()},
        metadata={},
    )


def make_strategy(
    source: BotModel,
    *,
    candles: DataFrame | None = None,
    extra_bots: list[BotModel] | None = None,
    current_qty: float = 0,
) -> tuple[TopGainerMomentumRecovery, SimpleNamespace, set[str]]:
    if candles is None:
        candles, btc_candles, _ = make_recovery_candles()
    else:
        btc_candles = candles.copy()
        btc_candles[["open", "high", "low", "close"]] = 100.0

    attempted_source_ids: set[str] = set()
    at_consumer = SimpleNamespace(
        autotrade_settings=SimpleNamespace(base_order_size=12.0),
        kucoin_futures_api=SimpleNamespace(
            get_futures_position=Mock(
                return_value=SimpleNamespace(current_qty=current_qty)
            )
        ),
        process_autotrade_restrictions=AsyncMock(),
    )
    context = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol=SYMBOL,
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.FUTURES,
        current_symbol_data=SimpleNamespace(
            base_asset="TEST",
            quote_asset="USDT",
        ),
        price_precision=4,
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=at_consumer,
        binbot_api=SimpleNamespace(get_active_pairs=Mock(return_value=[])),
        top_gainer_recovery_bots=[source, *(extra_bots or [])],
        top_gainer_recovery_attempted_source_ids=attempted_source_ids,
        df_15m=candles,
        df_btc_15m=btc_candles,
        latest_market_context=make_market_context(),
        finalize_signal_bot_params=Mock(),
        dispatch_signal_record=AsyncMock(),
    )
    return (
        TopGainerMomentumRecovery(cast(Any, context)),
        context,
        attempted_source_ids,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [Status.error, Status.inactive, Status.completed])
async def test_recovery_emits_for_each_eligible_source_outcome(status: Status) -> None:
    candles, _, source_created_at = make_recovery_candles()
    source = make_source_bot(status=status, created_at=source_created_at)
    strategy, context, attempted = make_strategy(source, candles=candles)

    await strategy.signal(
        current_price=111.0,
        bb_high=113.0,
        bb_mid=108.0,
        bb_low=103.0,
    )

    context.dispatch_signal_record.assert_awaited_once()
    emitted = context.dispatch_signal_record.call_args.kwargs["value"]
    assert emitted.bot_params.name == TopGainerMomentumRecovery.ALGO
    assert emitted.bot_params.fiat_order_size == 2.0
    assert emitted.bot_params.stop_loss == 2.0
    assert emitted.bot_params.trailing_profit == 2.0
    assert emitted.bot_params.trailing_deviation == 0.75
    assert TopGainerMomentumRecovery.source_log_marker(str(source.id)) in (
        emitted.bot_params.logs
    )
    assert attempted == {str(source.id)}
    context.at_consumer.process_autotrade_restrictions.assert_awaited_once_with(emitted)


@pytest.mark.asyncio
async def test_completed_non_stop_source_does_not_arm_recovery() -> None:
    candles, _, source_created_at = make_recovery_candles()
    source = make_source_bot(
        status=Status.completed,
        created_at=source_created_at,
        close_deal_type=DealType.take_profit,
    )
    strategy, context, _ = make_strategy(source, candles=candles)

    await strategy.signal(111.0, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_requires_a_bottoming_candle() -> None:
    candles, _, source_created_at = make_recovery_candles()
    candles.loc[len(candles) - 2, "open"] = 108.0
    source = make_source_bot(status=Status.error, created_at=source_created_at)
    strategy, context, _ = make_strategy(source, candles=candles)

    await strategy.signal(111.0, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_requires_breakout_reacceleration() -> None:
    candles, _, source_created_at = make_recovery_candles()
    candles.loc[len(candles) - 1, ["open", "high", "low", "close"]] = [
        107.5,
        108.0,
        106.8,
        107.8,
    ]
    source = make_source_bot(status=Status.inactive, created_at=source_created_at)
    strategy, context, _ = make_strategy(source, candles=candles)

    await strategy.signal(107.8, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_refuses_an_existing_exchange_position() -> None:
    candles, _, source_created_at = make_recovery_candles()
    source = make_source_bot(status=Status.error, created_at=source_created_at)
    strategy, context, attempted = make_strategy(
        source,
        candles=candles,
        current_qty=3,
    )

    await strategy.signal(111.0, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_not_awaited()
    assert attempted == set()


@pytest.mark.asyncio
async def test_recovery_refuses_an_active_bot_or_entry_order() -> None:
    candles, _, source_created_at = make_recovery_candles()
    source = make_source_bot(status=Status.error, created_at=source_created_at)
    strategy, context, attempted = make_strategy(source, candles=candles)
    context.binbot_api.get_active_pairs.return_value = [SYMBOL]

    await strategy.signal(111.0, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_not_awaited()
    context.at_consumer.kucoin_futures_api.get_futures_position.assert_not_called()
    assert attempted == set()


@pytest.mark.asyncio
async def test_recovery_attempts_each_source_only_once() -> None:
    candles, _, source_created_at = make_recovery_candles()
    source = make_source_bot(status=Status.error, created_at=source_created_at)
    strategy, context, _ = make_strategy(source, candles=candles)

    await strategy.signal(111.0, 113.0, 108.0, 103.0)
    await strategy.signal(111.0, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_awaited_once()


@pytest.mark.asyncio
async def test_existing_recovery_bot_persists_source_deduplication() -> None:
    candles, _, source_created_at = make_recovery_candles()
    source = make_source_bot(status=Status.error, created_at=source_created_at)
    recovery_bot = BotModel(
        id=UUID(int=2),
        pair=SYMBOL,
        name=TopGainerMomentumRecovery.ALGO,
        status=Status.completed,
        market_type=MarketType.FUTURES,
        created_at=(source_created_at + INTERVAL_MS) / 1000,
        updated_at=(source_created_at + INTERVAL_MS) / 1000,
        logs=[TopGainerMomentumRecovery.source_log_marker(str(source.id))],
    )
    strategy, context, _ = make_strategy(
        source,
        candles=candles,
        extra_bots=[recovery_bot],
    )

    await strategy.signal(111.0, 113.0, 108.0, 103.0)

    context.dispatch_signal_record.assert_not_awaited()


def test_inactive_source_with_unclosed_exposure_is_not_eligible() -> None:
    _, _, source_created_at = make_recovery_candles()
    source = make_source_bot(
        status=Status.inactive,
        created_at=source_created_at,
        open_exposure=True,
    )

    selected = TopGainerMomentumRecovery.select_source(
        bots=[source],
        symbol=SYMBOL,
        attempted_source_ids=set(),
        now_ms=int(datetime.now(UTC).timestamp() * 1000),
    )

    assert selected is None


def test_source_created_at_seconds_are_compared_as_milliseconds() -> None:
    candles, btc_candles, source_created_at = make_recovery_candles()
    source = make_source_bot(status=Status.error, created_at=source_created_at)
    now_ms = source_created_at + TopGainerMomentumRecovery.WATCH_WINDOW_MS

    selected = TopGainerMomentumRecovery.select_source(
        bots=[source],
        symbol=SYMBOL,
        attempted_source_ids=set(),
        now_ms=now_ms,
    )
    setup, reason = TopGainerMomentumRecovery.recovery_setup(
        df=candles,
        btc_df=btc_candles,
        source=source,
    )

    assert source.created_at == pytest.approx(source_created_at / 1000)
    assert selected is source
    assert reason == "top_gainer_recovery_reaccelerated"
    assert setup is not None
    assert setup["source_created_at"] == source_created_at


def test_newest_eligible_source_supersedes_older_source() -> None:
    _, _, source_created_at = make_recovery_candles()
    older = make_source_bot(
        status=Status.error,
        created_at=source_created_at - INTERVAL_MS,
        bot_id=1,
    )
    newer = make_source_bot(
        status=Status.inactive,
        created_at=source_created_at,
        bot_id=2,
    )

    selected = TopGainerMomentumRecovery.select_source(
        bots=[older, newer],
        symbol=SYMBOL,
        attempted_source_ids=set(),
        now_ms=int(datetime.now(UTC).timestamp() * 1000),
    )

    assert selected is not None
    assert selected.id == newer.id

from asyncio import CancelledError, sleep
from dataclasses import asdict
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from kucoin_universal_sdk.generate.futures.market import GetPartOrderBookResp
from pybinbot import ExchangeId, KucoinFutures, MarketType

from producers.context_evaluator import ContextEvaluator
from shared.order_book_liquidity import (
    LiquidityAssessment,
    assess_futures_order_book,
)
from strategies.dark_knight import DarkKnight
from strategies.top_gainer_short import TopGainerShort
from tests.test_dark_knight import context as dark_knight_context
from tests.test_top_gainer_short import NOW, make_context


def book(**changes) -> GetPartOrderBookResp:
    fields = {
        "symbol": "TESTUSDTM",
        "ts": int(NOW.timestamp() * 1_000_000_000),
        "bids": [[99.9, 1_000]],
        "asks": [[100.1, 1_000]],
    }
    fields.update(changes)
    return GetPartOrderBookResp(**fields)


def assess(snapshot=None, **changes) -> LiquidityAssessment:
    arguments: dict[str, Any] = {
        "symbol": "TESTUSDTM",
        "multiplier": 0.1,
        "position_notional": 40.0,
        "now_seconds": NOW.timestamp(),
    }
    arguments.update(changes)
    return assess_futures_order_book(snapshot or book(), **arguments)


def test_depth_is_contract_notional_on_each_side_not_contract_count_or_margin():
    result = assess()
    assert result.allowed
    assert result.bid_depth_usdt == pytest.approx(9_990)
    assert result.ask_depth_usdt == pytest.approx(10_010)
    assert result.required_side_depth_usdt == 5_000
    assert not assess(multiplier=0.001).allowed
    assert assess(multiplier=100).allowed


def test_far_away_order_walls_do_not_make_a_thin_book_pass():
    result = assess(book(bids=[[99.9, 1], [99.4, 1_000_000]], asks=[[100.1, 1_000]]))
    assert result.reason == "insufficient_bid_depth"
    assert result.bid_depth_usdt == pytest.approx(9.99)


@pytest.mark.parametrize("side", ["bids", "asks"])
def test_short_requires_depth_for_both_entry_and_buy_to_cover(side):
    snapshot = book()
    setattr(snapshot, side, [[getattr(snapshot, side)[0][0], 1]])
    assert not assess(snapshot).allowed
    assert assess(snapshot).reason == f"insufficient_{side[:-1]}_depth"


def test_wide_spread_blocks_even_when_both_sides_have_large_orders():
    result = assess(book(bids=[[99.8, 100_000]], asks=[[100.2, 100_000]]))
    assert not result.allowed
    assert result.reason == "spread_too_wide"


def test_depth_requirement_scales_with_proposed_position_notional():
    result = assess(position_notional=600)
    assert result.required_side_depth_usdt == 12_000
    assert not result.allowed
    assert assess(position_notional=400).allowed


def test_exact_policy_boundaries_are_inclusive():
    snapshot = book(bids=[[99.85, 5_000 / 99.85]], asks=[[100.15, 5_000 / 100.15]])
    assert assess(snapshot, multiplier=1).allowed
    assert not assess(snapshot, multiplier=0.999).allowed


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"bids": []}, "order_book_side_empty"),
        ({"asks": None}, "order_book_side_empty"),
        ({"symbol": "OTHERUSDTM"}, "order_book_symbol_mismatch"),
        ({"ts": None}, "order_book_timestamp_missing"),
        ({"ts": int((NOW.timestamp() - 11) * 1e9)}, "order_book_stale_or_future"),
        ({"ts": int((NOW.timestamp() + 3) * 1e9)}, "order_book_stale_or_future"),
        ({"ts": int(NOW.timestamp() * 1000)}, "order_book_stale_or_future"),
        ({"bids": [[99.9]]}, "order_book_levels_invalid"),
        ({"bids": [[99.9, 0]]}, "order_book_levels_invalid"),
        ({"bids": [[99.9, -1]]}, "order_book_levels_invalid"),
        ({"bids": [[float("nan"), 1]]}, "order_book_levels_invalid"),
        ({"asks": [[100.1, float("inf")]]}, "order_book_levels_invalid"),
        ({"bids": [[99, 1], [99.9, 1]]}, "order_book_levels_unordered"),
        ({"asks": [[100.2, 1], [100.1, 1]]}, "order_book_levels_unordered"),
        ({"bids": [[99.9, 1_000], [99.9, 1_000]]}, "order_book_levels_unordered"),
        ({"bids": [[100.1, 1_000]]}, "order_book_crossed"),
    ],
)
def test_unusable_exchange_snapshots_block_entries(changes, reason):
    result = assess(book(**changes))
    assert not result.allowed
    assert result.reason == reason


@pytest.mark.parametrize("field", ["multiplier", "position_notional"])
@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
def test_invalid_contract_sizing_never_passes(field, value):
    assert not assess(**{field: value}).allowed


@pytest.fixture
def evaluator(monkeypatch):
    monkeypatch.setattr("producers.context_evaluator.time", lambda: NOW.timestamp())
    instance = object.__new__(ContextEvaluator)
    instance.symbol = "TESTUSDTM"
    instance.exchange = ExchangeId.KUCOIN
    instance.market_type = MarketType.FUTURES
    instance.current_symbol_data = make_context().current_symbol_data
    instance.current_symbol_data.multiplier = 0.1
    instance.api = Mock(spec=KucoinFutures)
    instance.api.futures_market_api = SimpleNamespace(
        get_part_order_book=Mock(return_value=book())
    )
    instance._liquidity_book = None
    instance._liquidity_checked_at = float("-inf")
    return instance


@pytest.mark.asyncio
async def test_context_reuses_snapshot_but_reassesses_each_position_size(evaluator):
    assert (await evaluator.assess_futures_liquidity(40)).allowed
    assert not (await evaluator.assess_futures_liquidity(600)).allowed
    fetch = evaluator.api.futures_market_api.get_part_order_book
    fetch.assert_called_once()
    assert fetch.call_args.args[0].symbol == "TESTUSDTM"
    assert fetch.call_args.args[0].size == "100"


@pytest.mark.asyncio
async def test_expired_cache_is_refetched_and_failure_does_not_reuse_good_book(
    evaluator, monkeypatch
):
    monkeypatch.setattr("producers.context_evaluator.monotonic", lambda: 100.0)
    assert (await evaluator.assess_futures_liquidity(40)).allowed
    monkeypatch.setattr("producers.context_evaluator.monotonic", lambda: 103.0)
    fetch = evaluator.api.futures_market_api.get_part_order_book
    fetch.side_effect = RuntimeError("exchange unavailable")
    assert (
        await evaluator.assess_futures_liquidity(40)
    ).reason == "order_book_unavailable"
    assert not (await evaluator.assess_futures_liquidity(40)).allowed
    assert fetch.call_count == 2


@pytest.mark.asyncio
async def test_slow_exchange_request_blocks_entry_without_stalling_pipeline(
    evaluator, monkeypatch
):
    async def delayed(*_args):
        await sleep(1)
        return book()

    monkeypatch.setattr("producers.context_evaluator.to_thread", delayed)
    evaluator.ORDER_BOOK_TIMEOUT_SECONDS = 0.001
    assert (
        await evaluator.assess_futures_liquidity(40)
    ).reason == "order_book_unavailable"


@pytest.mark.asyncio
async def test_pipeline_cancellation_is_not_swallowed_by_liquidity_gate(
    evaluator, monkeypatch
):
    monkeypatch.setattr(
        "producers.context_evaluator.to_thread", AsyncMock(side_effect=CancelledError)
    )
    with pytest.raises(CancelledError):
        await evaluator.assess_futures_liquidity(40)


@pytest.mark.asyncio
async def test_stale_exchange_timestamp_blocks_even_a_newly_fetched_book(evaluator):
    evaluator.api.futures_market_api.get_part_order_book.return_value = book(
        ts=int((NOW.timestamp() - 11) * 1e9)
    )
    assert (
        await evaluator.assess_futures_liquidity(40)
    ).reason == "order_book_stale_or_future"


@pytest.mark.asyncio
async def test_unsupported_contract_market_cannot_bypass_gate(evaluator):
    evaluator.current_symbol_data.quote_asset = "USD"
    assert not (await evaluator.assess_futures_liquidity(40)).allowed
    evaluator.api.futures_market_api.get_part_order_book.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy_type", [TopGainerShort, DarkKnight])
async def test_rejected_liquidity_prevents_all_emissions_and_can_retry_same_setup(
    strategy_type, monkeypatch
):
    for module in ("top_gainer_short", "dark_knight", "lower_high_pattern"):
        monkeypatch.setattr(f"strategies.{module}.time", lambda: NOW.timestamp())
    context = dark_knight_context()
    context.assess_futures_liquidity.return_value = LiquidityAssessment(
        False, "insufficient_ask_depth"
    )
    strategy = strategy_type(cast(Any, context))
    await strategy.signal(135.0, 140, 132, 120)
    context.assess_futures_liquidity.assert_awaited_once()
    context.dispatch_signal_record.assert_not_awaited()
    context.telegram_consumer.dispatch_signal.assert_not_awaited()
    context.at_consumer.process_autotrade_restrictions.assert_not_awaited()
    assert not context.strategy_cooldowns

    context.assess_futures_liquidity.return_value = assess()
    await strategy.signal(135.0, 140, 132, 120)
    context.dispatch_signal_record.assert_awaited_once()
    indicators = context.dispatch_signal_record.await_args.kwargs["indicators"]
    assert indicators["order_book_liquidity"] == asdict(assess())


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy_type", [TopGainerShort, DarkKnight])
async def test_gate_uses_final_margin_times_proposed_leverage(
    strategy_type, monkeypatch
):
    for module in ("top_gainer_short", "dark_knight", "lower_high_pattern"):
        monkeypatch.setattr(f"strategies.{module}.time", lambda: NOW.timestamp())
    context = dark_knight_context()

    await strategy_type(cast(Any, context)).signal(135.0, 140, 132, 120)
    value = context.dispatch_signal_record.await_args.kwargs["value"]
    context.assess_futures_liquidity.assert_awaited_once_with(
        value.bot_params.fiat_order_size * value.bot_params.futures_leverage
    )
    if strategy_type is TopGainerShort:
        assert value.bot_params.fiat_order_size == 6.0

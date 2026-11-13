from typing import Any
import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

import main as binquant_main


@pytest.mark.asyncio
async def test_main_requires_the_gil_disabled_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = Mock()
    monkeypatch.setattr(binquant_main, "WebsocketClientFactory", factory)
    monkeypatch.setattr(binquant_main.sys, "_is_gil_enabled", lambda: True)

    with pytest.raises(RuntimeError, match="requires free-threaded Python"):
        await binquant_main.main()

    factory.assert_not_called()


@pytest.mark.asyncio
async def test_main_bootstraps_websocket_subscriptions_before_consuming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakeFactory:
        def __init__(self, queue: Any, liquidation_store: Any) -> None:
            self.queue = queue
            self.liquidation_store = liquidation_store

        async def create_connector(self) -> list[str]:
            events.append("subscriptions_ready")
            return ["client"]

    class FakeKlinesProvider:
        def __init__(self, calculation_pool: Any, liquidation_store: Any) -> None:
            self.calculation_pool = calculation_pool
            self.liquidation_store = liquidation_store

    async def fake_ingest_loop(clients: list[str]) -> None:
        assert clients == ["client"]
        events.append("ingest_started")

    async def fake_consume_loop(
        _klines_provider: FakeKlinesProvider,
        _queue: Any,
    ) -> None:
        events.append("consumer_started")

    monkeypatch.setattr(binquant_main, "WebsocketClientFactory", FakeFactory)
    monkeypatch.setattr(binquant_main, "KlinesProvider", FakeKlinesProvider)
    monkeypatch.setattr(binquant_main, "ingest_loop", fake_ingest_loop)
    monkeypatch.setattr(binquant_main, "consume_loop", fake_consume_loop)

    await binquant_main.main()

    assert events == [
        "subscriptions_ready",
        "ingest_started",
        "consumer_started",
    ]


@pytest.mark.asyncio
async def test_ingest_loop_runs_connected_clients() -> None:
    events: list[str] = []

    class FakeClient:
        def __init__(self, name: str) -> None:
            self.name = name

        async def run_forever(self) -> None:
            events.append(self.name)

    await binquant_main.ingest_loop([FakeClient("first"), FakeClient("second")])

    assert events == ["first", "second"]


@pytest.mark.asyncio
async def test_main_closes_calculation_workers_when_subscription_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = Mock()
    monkeypatch.setattr(binquant_main, "CandleCalculationPool", Mock(return_value=pool))
    monkeypatch.setattr(binquant_main, "KlinesProvider", Mock())
    factory = Mock()
    factory.create_connector = AsyncMock(
        side_effect=RuntimeError("Subscriptions failed")
    )
    monkeypatch.setattr(
        binquant_main, "WebsocketClientFactory", Mock(return_value=factory)
    )

    with pytest.raises(RuntimeError, match="Subscriptions failed"):
        await binquant_main.main()

    pool.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_messages_finish_in_order_before_the_next_strategy_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait("first")
    queue.put_nowait("second")
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    events: list[str] = []

    async def aggregate(message: str) -> None:
        events.append(f"start:{message}")
        if message == "first":
            first_started.set()
            await release_first.wait()
        events.append(f"finish:{message}")

    provider: Any = Mock()
    provider.load_data_on_start = AsyncMock()
    provider.aggregate_data = aggregate
    monkeypatch.setattr(binquant_main, "touch_heartbeat", Mock())
    consumer = asyncio.create_task(binquant_main.consume_loop(provider, queue))
    try:
        await asyncio.wait_for(first_started.wait(), timeout=5)
        assert events == ["start:first"]
        release_first.set()
        await asyncio.wait_for(queue.join(), timeout=5)
        assert events == [
            "start:first",
            "finish:first",
            "start:second",
            "finish:second",
        ]
    finally:
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer

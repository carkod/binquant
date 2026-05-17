from typing import Any

import pytest

import main as binquant_main


@pytest.mark.asyncio
async def test_main_bootstraps_websocket_subscriptions_before_consuming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakeFactory:
        def __init__(self, queue: Any) -> None:
            self.queue = queue

        async def create_connector(self) -> list[str]:
            events.append("subscriptions_ready")
            return ["client"]

    class FakeKlinesProvider:
        pass

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

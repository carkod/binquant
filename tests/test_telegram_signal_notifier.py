from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from shared.telegram_signal_notifier import TelegramSignalNotifier


def notifier() -> Any:
    result = cast(Any, TelegramSignalNotifier())
    result.delivery.send = AsyncMock(return_value=True)
    return result


@pytest.mark.asyncio
async def test_matching_signals_are_deduplicated_despite_price_changes():
    sender = notifier()
    message = """
        - [production] <strong>#top_gainer_short algorithm</strong> #TAUSDTM
        - Action: LONG ENTRY
        - Current price: 0.071
        - Strategy: top_gainer_short
        - Autotrade route: market_trend_up_symbol_trend_up
        - Autotrade is enabled
    """

    assert await sender.dispatch_signal(message) is True
    assert await sender.dispatch_signal(message.replace("0.071", "0.074")) is False
    sender.delivery.send.assert_awaited_once()
    sent = sender.delivery.send.await_args.args[0]
    assert sent.splitlines()[0].startswith("- [production]")
    assert not any(line != line.strip() for line in sent.splitlines())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "algorithm", ["higher_low_pattern", "top_gainer_early_momentum"]
)
async def test_disabled_strategies_do_not_send_signals(algorithm):
    sender = notifier()
    assert (
        await sender.dispatch_signal(
            f"<strong>#{algorithm} algorithm</strong> #TAUSDTM"
        )
        is False
    )
    sender.delivery.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "algorithm", ["top_gainer_short", "dark_knight", "top_loser_early_momentum"]
)
async def test_selected_strategies_send_signals(algorithm):
    sender = notifier()
    assert (
        await sender.dispatch_signal(
            f"<strong>#{algorithm} algorithm</strong> #TAUSDTM"
        )
        is True
    )
    sender.delivery.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_disabled_notifications_do_not_send_signals():
    sender = notifier()
    sender.is_enabled = False
    assert (
        await sender.dispatch_signal(
            "<strong>#top_gainer_short algorithm</strong> #TAUSDTM"
        )
        is False
    )
    sender.delivery.send.assert_not_awaited()

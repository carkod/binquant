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
    "heading",
    [
        "top_gainer_short algorithm",
        "dark_knight algorithm",
        "top_loser_early_momentum algorithm",
        "lower_high_pattern pattern",
        "higher_low_pattern pattern",
        "fastest_movers algorithm",
        "another_strategy algorithm",
    ],
)
async def test_signal_messages_are_delivered_without_pacing_or_retries(heading):
    sender = notifier()
    message = f"<strong>#{heading}</strong> #TAUSDTM"
    assert await sender.dispatch_signal(message) is True
    sender.delivery.send.assert_awaited_once_with(
        message, expires_at=None, retry_on_rate_limit=False
    )


@pytest.mark.asyncio
async def test_different_patterns_on_the_same_symbol_have_independent_duplicates():
    sender = notifier()
    lower = "<strong>#lower_high_pattern pattern</strong> #TAUSDTM\n- Event ID: 100"
    higher = "<strong>#higher_low_pattern pattern</strong> #TAUSDTM\n- Event ID: 100"
    assert await sender.dispatch_signal(lower) is True
    assert await sender.dispatch_signal(higher) is True
    assert await sender.dispatch_signal(lower) is False
    assert await sender.dispatch_signal(higher) is False
    assert await sender.dispatch_signal(lower.replace("100", "200")) is True
    assert sender.delivery.send.await_count == 3


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

from typing import Any
from unittest.mock import AsyncMock

import pytest

from notifiers.telegram_signal_notifier import TelegramSignalNotifier
from tests.test_higher_low_pattern import make_algo as make_higher_low
from tests.test_higher_low_pattern import make_higher_low_df


@pytest.mark.asyncio
async def test_confirmed_higher_low_reaches_telegram_once_across_evaluator_instances(
    monkeypatch,
):
    notifier = TelegramSignalNotifier()
    send = AsyncMock(return_value=True)
    monkeypatch.setattr(notifier.delivery, "send", send)
    cooldowns: dict[tuple[str, str], int] = {}

    for _ in range(2):
        pattern: Any = make_higher_low(
            make_higher_low_df(), strategy_cooldowns=cooldowns
        )
        pattern.telegram_notifier = notifier
        await pattern.signal()

    send.assert_awaited_once()
    assert "#higher_low_pattern pattern" in send.await_args_list[0].args[0]

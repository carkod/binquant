from typing import Any
from unittest.mock import AsyncMock

import pytest

from shared.telegram_signal_notifier import TelegramSignalNotifier
from tests.test_higher_low_pattern import make_algo as make_higher_low
from tests.test_higher_low_pattern import make_higher_low_df
from tests.test_lower_high_pattern import make_algo as make_lower_high
from tests.test_lower_high_pattern import make_lower_high_df


@pytest.mark.asyncio
async def test_confirmed_patterns_reach_telegram_once_across_evaluator_instances(
    monkeypatch,
):
    notifier = TelegramSignalNotifier()
    send = AsyncMock(return_value=True)
    monkeypatch.setattr(notifier.delivery, "send", send)
    cooldowns: dict[tuple[str, str], int] = {}

    for create, dataframe in (
        (make_lower_high, make_lower_high_df()),
        (make_higher_low, make_higher_low_df()),
    ):
        for _ in range(2):
            pattern: Any = create(dataframe, strategy_cooldowns=cooldowns)
            pattern.telegram_notifier = notifier
            await pattern.signal()

    assert send.await_count == 2
    assert "#lower_high_pattern pattern" in send.await_args_list[0].args[0]
    assert "#higher_low_pattern pattern" in send.await_args_list[1].args[0]

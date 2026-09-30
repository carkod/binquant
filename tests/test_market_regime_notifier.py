from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from shared.macroregime_directional_notifier import (
    MacroregimeDirectionalNotifier,
)


def make_algo(
    current: str | None,
    *,
    previous: str | None = None,
) -> MacroregimeDirectionalNotifier:
    evaluator = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        symbol="TESTUSDT",
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        macroregime_directional=current,
        macroregime_oscillation_intensity=0.73,
        microregime_directional="NONE",
        microregime_oscillation_intensity=0.64,
        last_macroregime_directional=previous,
        context_timestamp_line=Mock(
            return_value="- Context timestamp: 1970-01-01 00:00:02 UTC"
        ),
        regime_telegram_lines=Mock(
            return_value=(
                "- Macro directional (BTC): DOWN\n"
                "- Macro oscillation intensity (BTC): 0.73\n"
                "- Micro directional (TESTUSDT): NONE\n"
                "- Micro oscillation intensity (TESTUSDT): 0.64"
            )
        ),
    )
    return MacroregimeDirectionalNotifier(cast(Any, evaluator))


@pytest.mark.asyncio
async def test_macroregime_notifier_bootstraps_without_emitting_transition() -> None:
    algo = make_algo("UP")

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]
    assert algo.last_macroregime_directional == "UP"
    assert algo.context_evaluator.last_macroregime_directional == "UP"


@pytest.mark.asyncio
async def test_macroregime_notifier_emits_directional_transition() -> None:
    algo = make_algo("DOWN", previous="UP")

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_called_once()  # type: ignore[attr-defined]
    message = algo.telegram_consumer.dispatch_signal.call_args.args[0]  # type: ignore[attr-defined]
    assert "#macroregime_directional_transition" in message
    assert "Macro directional transition: UP -> DOWN" in message
    assert "Macro oscillation intensity (BTC): 0.73" in message
    assert "Micro directional (TESTUSDT): NONE" in message
    assert "Micro oscillation intensity (TESTUSDT): 0.64" in message
    assert "Context timestamp: 1970-01-01 00:00:02 UTC" in message


@pytest.mark.asyncio
async def test_macroregime_notifier_skips_unchanged_direction() -> None:
    algo = make_algo("NONE", previous="NONE")

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_macroregime_notifier_skips_unavailable_btc_measure() -> None:
    algo = make_algo(None, previous="UP")

    await algo.signal()

    algo.telegram_consumer.dispatch_signal.assert_not_called()  # type: ignore[attr-defined]

from typing import Any

from pandas import DataFrame
import pytest

from producers.context_evaluator import ContextEvaluator


def closes(values: list[float]) -> DataFrame:
    return DataFrame({"close": values})


def test_monotonic_path_is_directional_with_no_oscillation() -> None:
    directional, oscillation = ContextEvaluator.assess_directional_and_oscillation(
        closes([100.0 + index for index in range(96)])
    )

    assert directional == "UP"
    assert oscillation == 0.0


def test_active_zigzag_is_non_directional_with_high_oscillation() -> None:
    values = [100.0 if index % 2 == 0 else 102.0 for index in range(96)]

    directional, oscillation = ContextEvaluator.assess_directional_and_oscillation(
        closes(values)
    )

    assert directional == "NONE"
    assert oscillation is not None
    assert oscillation > 0.95


def test_quiet_flat_path_is_not_mistaken_for_active_oscillation() -> None:
    values = [100.0 if index % 2 == 0 else 100.002 for index in range(96)]

    directional, oscillation = ContextEvaluator.assess_directional_and_oscillation(
        closes(values)
    )

    assert directional == "NONE"
    assert oscillation is not None
    assert oscillation < 0.05


def test_downward_efficient_path_is_directional_down() -> None:
    directional, oscillation = ContextEvaluator.assess_directional_and_oscillation(
        closes([200.0 - index for index in range(96)])
    )

    assert directional == "DOWN"
    assert oscillation == 0.0


def test_regime_measure_requires_enough_history() -> None:
    assert ContextEvaluator.assess_directional_and_oscillation(
        closes([100.0] * 95)
    ) == (None, None)


def test_macro_and_micro_use_same_calculation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 2_000_000
    monkeypatch.setattr(
        "producers.context_evaluator.time",
        lambda: now_ms / 1000,
    )
    evaluator: Any = object.__new__(ContextEvaluator)
    evaluator.symbol = "ALTUSDT"
    evaluator.df_btc_15m = DataFrame(
        {
            "close": [100.0 + index for index in range(96)],
            "close_time": range(1_000_000, 1_000_096),
        }
    )
    evaluator.df_15m = DataFrame(
        {
            "close": [100.0 if index % 2 == 0 else 102.0 for index in range(96)],
            "close_time": range(1_000_000, 1_000_096),
        }
    )

    evaluator.refresh_regime_measures()

    assert evaluator.macroregime_directional == "UP"
    assert evaluator.macroregime_oscillation_intensity == 0.0
    assert evaluator.microregime_directional == "NONE"
    assert evaluator.microregime_oscillation_intensity is not None
    assert evaluator.microregime_oscillation_intensity > 0.95
    assert evaluator.regime_measures() == {
        "macroregime_directional": "UP",
        "macroregime_oscillation_intensity": 0.0,
        "microregime_directional": "NONE",
        "microregime_oscillation_intensity": (
            evaluator.microregime_oscillation_intensity
        ),
    }

    message = evaluator.regime_telegram_lines()
    assert "Macro directional (BTC): UP" in message
    assert "Macro oscillation intensity (BTC): 0.0" in message
    assert "Micro directional (ALTUSDT): NONE" in message
    assert "Micro oscillation intensity (ALTUSDT):" in message


def test_regime_measures_exclude_forming_btc_and_symbol_candles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 2_000_000
    monkeypatch.setattr(
        "producers.context_evaluator.time",
        lambda: now_ms / 1000,
    )
    completed_closes = [100.0 + index for index in range(96)]
    frame = DataFrame(
        {
            "close": [*completed_closes, 1.0],
            "close_time": [*range(1_000_000, 1_000_096), now_ms + 1],
        }
    )
    evaluator: Any = object.__new__(ContextEvaluator)
    evaluator.df_btc_15m = frame.copy()
    evaluator.df_15m = frame.copy()

    evaluator.refresh_regime_measures()

    assert evaluator.macroregime_directional == "UP"
    assert evaluator.macroregime_oscillation_intensity == 0.0
    assert evaluator.microregime_directional == "UP"
    assert evaluator.microregime_oscillation_intensity == 0.0

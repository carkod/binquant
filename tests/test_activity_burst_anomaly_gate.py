import numpy as np
import pytest
from pandas import DataFrame

from strategies.activity_burst_anomaly_gate import ActivityBurstAnomalyGate


def make_feature_frame(rows: int = 121) -> DataFrame:
    rng = np.random.default_rng(2024)
    frame = DataFrame(
        {
            "price_jump": rng.normal(0.001, 0.002, rows),
            "volume_ratio": rng.normal(1.0, 0.08, rows),
            "quote_volume_ratio": rng.normal(1.0, 0.08, rows),
            "range_frac": rng.normal(0.008, 0.001, rows),
            "body_frac": rng.normal(0.45, 0.05, rows),
            "close_to_high": rng.normal(0.5, 0.05, rows),
            "recent_up_closes": rng.integers(0, 4, rows),
        }
    )
    frame.loc[frame.index[-1]] = {
        "price_jump": 0.08,
        "volume_ratio": 12.0,
        "quote_volume_ratio": 11.0,
        "range_frac": 0.09,
        "body_frac": 0.95,
        "close_to_high": 0.01,
        "recent_up_closes": 3,
    }
    return frame


def test_evaluate_excludes_current_bar_from_initial_batch() -> None:
    frame = make_feature_frame()
    gate = ActivityBurstAnomalyGate()

    result = gate.evaluate(frame)

    assert result is not None
    assert result.training_rows == len(frame) - 1
    assert gate.scaler is not None
    expected_training = gate._feature_matrix(frame).iloc[:-1]
    assert gate.scaler.mean_ == pytest.approx(expected_training.mean().to_numpy())
    assert result.pca_confirmed is True
    assert result.isolation_forest_confirmed is True
    assert result.gate_passed is True


def test_evaluate_keeps_initial_scaler_and_models_fixed() -> None:
    frame = make_feature_frame()
    gate = ActivityBurstAnomalyGate()
    first = gate.evaluate(frame)
    assert first is not None

    changed_history = frame.copy()
    changed_history.loc[changed_history.index[:-1], "price_jump"] = 100.0
    changed_history.loc[changed_history.index[:-1], "volume_ratio"] = 1000.0
    second = gate.evaluate(changed_history)

    assert second is not None
    assert second.pca_score == pytest.approx(first.pca_score)
    assert second.pca_percentile == pytest.approx(first.pca_percentile)
    assert second.isolation_forest_score == pytest.approx(first.isolation_forest_score)
    assert second.isolation_forest_percentile == pytest.approx(
        first.isolation_forest_percentile
    )


def test_evaluate_requires_full_initial_batch() -> None:
    gate = ActivityBurstAnomalyGate()

    assert gate.evaluate(make_feature_frame(rows=100)) is None
    assert gate.is_fitted is False

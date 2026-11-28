import pytest

from shared.trend import regression_slope


@pytest.mark.parametrize("slope", [-2.0, 0.0, 2.0])
@pytest.mark.parametrize("origin", [-100.0, 0.0, 100.0])
def test_regression_measures_direction_and_units_across_signed_values(slope, origin):
    samples = [
        (1_700_000_000 + hour * 3600, origin + slope * hour) for hour in range(40)
    ]

    assert regression_slope(samples) == pytest.approx(slope)


def test_regression_uses_elapsed_time_for_irregular_observations():
    samples = [(0, 10), (3600, 12), (10800, 16), (14400, 18)]

    assert regression_slope(samples) == pytest.approx(2)


@pytest.mark.parametrize(
    "samples",
    [
        [],
        [(0, 1), (1, 2)],
        [(0, 1), (0, 2), (1, 3)],
        [(0, 1), (2, 2), (1, 3)],
        [(0, 1), (1, float("nan")), (2, 3)],
        [(0, 1), (float("inf"), 2), (2, 3)],
    ],
)
def test_invalid_observations_cannot_establish_direction(samples):
    assert regression_slope(samples) is None

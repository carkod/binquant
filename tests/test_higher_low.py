import pytest

from shared.higher_low import confirmed_higher_low


def samples(*, second_trough=106.0, falling=False, offset=0.0):
    values = (
        [125 - i * 0.1 for i in range(30)]
        if falling
        else [90 + i * 0.1 for i in range(30)]
    )
    values += [110, 105, 100, 105, 110, 112, 109, second_trough, 109, 112]
    return [(i * 900.0, value + offset) for i, value in enumerate(values)]


def detect(points):
    return confirmed_higher_low(
        points,
        now_seconds=39 * 900,
        lookback_samples=40,
        fractal_wing=2,
        max_age_seconds=1800,
        max_gap_seconds=900,
    )


@pytest.mark.parametrize(
    "second_trough,passes", [(104.99, False), (105, False), (105.01, True)]
)
def test_confirmed_higher_low_requires_more_than_five_percent_rise(
    second_trough, passes
):
    result = detect(samples(second_trough=second_trough))

    assert (result is not None) is passes
    if result is not None:
        assert result["trend_slope_per_hour"] > 0


def test_two_higher_troughs_cannot_override_a_falling_full_window():
    assert detect(samples(falling=True)) is None


@pytest.mark.parametrize("offset", [-110.0, -100.0, 0.0])
def test_confirmed_higher_low_supports_negative_zero_and_positive_first_trough(offset):
    result = detect(samples(offset=offset))

    assert result is not None
    assert result["higher_low_first_trough"] == 100 + offset
    assert result["higher_low_rise"] == 6
    assert result["trend_slope_per_hour"] > 0


def test_breaching_the_second_trough_still_invalidates_an_upward_trend():
    points = samples()
    points[-1] = (points[-1][0], 105)

    assert detect(points) is None

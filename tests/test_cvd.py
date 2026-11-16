import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest

from shared.cvd import calculate_cvd_direction, cvd_telegram_line
from tests.cvd_fixtures import cvd_snapshot

NOW = datetime(2026, 11, 16, 19, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "values,direction",
    [
        ([1000, 850, 900, 700, 600, 500], "falling"),
        ([0, 150, 100, 300, 400, 500], "rising"),
        ([0, 0, 0, 0, 0, 0], "no_direction"),
        ([100, 100, 100, 100, 100], "no_direction"),
        (
            [
                39.832,
                246.773,
                26.381,
                120.192,
                90.736,
                140.38,
                32.11,
                106.58,
                131.251,
                121.781,
                92.203,
                133.149,
            ],
            "no_direction",
        ),
    ],
)
def test_direction_allows_rebounds_and_distinguishes_chop_and_constant_cvd(
    values, direction
):
    snapshot = cvd_snapshot(values, now=NOW)
    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )

    assert result.direction == direction
    assert result.sample_count == len(values)
    assert result.pearson_threshold == 0.7
    json.dumps(asdict(result), allow_nan=False)
    if len(set(values)) == 1:
        assert result.slope_usdt_per_minute == 0
        assert result.pearson_r == 0


def test_symbol_and_market_direction_use_same_score_and_slope_units():
    values = [0, 3, 1, 5, 6, 9]
    symbol = calculate_cvd_direction(
        cvd_snapshot(values, now=NOW), interval_seconds=60, now_seconds=NOW.timestamp()
    )
    market = calculate_cvd_direction(
        cvd_snapshot(values, now=NOW, interval_seconds=900),
        interval_seconds=900,
        now_seconds=NOW.timestamp(),
    )

    assert symbol.direction == market.direction == "rising"
    assert symbol.pearson_r == pytest.approx(market.pearson_r)
    assert market.slope_usdt_per_minute is not None
    assert symbol.slope_usdt_per_minute == pytest.approx(
        market.slope_usdt_per_minute * 15
    )
    assert symbol.window_end != market.window_end


@pytest.mark.parametrize("scale,offset", [(1, -10000), (1000000, 20000), (0.001, 0)])
def test_pearson_direction_is_invariant_to_symbol_scale_and_cvd_rebasing(scale, offset):
    values = [10, 8, 9, 5, 4, 2]
    baseline = calculate_cvd_direction(
        cvd_snapshot(values, now=NOW), interval_seconds=60, now_seconds=NOW.timestamp()
    )
    result = calculate_cvd_direction(
        cvd_snapshot([v * scale + offset for v in values], now=NOW),
        interval_seconds=60,
        now_seconds=NOW.timestamp(),
    )

    assert result.direction == baseline.direction == "falling"
    assert result.pearson_r == pytest.approx(baseline.pearson_r)
    assert baseline.slope_usdt_per_minute is not None
    assert result.slope_usdt_per_minute == pytest.approx(
        baseline.slope_usdt_per_minute * scale
    )


def test_noisy_positive_slope_requires_pearson_confirmation():
    snapshot = cvd_snapshot(
        [
            39.832,
            246.773,
            26.381,
            120.192,
            90.736,
            140.38,
            32.11,
            106.58,
            131.251,
            121.781,
            92.203,
            133.149,
        ],
        now=NOW,
    )
    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )

    assert result.slope_usdt_per_minute == pytest.approx(1.040958041958)
    assert result.pearson_r == pytest.approx(0.0626983183915)
    assert result.direction == "no_direction"


def test_partial_snapshot_can_still_supply_completed_history():
    snapshot = cvd_snapshot(list(range(12)), now=NOW)
    snapshot.partial = True
    for offset in range(1, 4):
        point = type(snapshot.data[-1])(**vars(snapshot.data[-1]))
        point.timestamp = snapshot.data[11].timestamp + timedelta(minutes=offset)
        point.complete = False
        point.gap = True
        point.current = offset == 3
        point.cvd = -100000
        snapshot.data.append(point)
    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )

    assert result.direction == "rising"
    assert result.sample_count == 12
    assert result.slope_usdt_per_minute == pytest.approx(1)


def test_regression_uses_at_most_twelve_latest_completed_points():
    snapshot = cvd_snapshot([100000, 100000] + list(range(12)), now=NOW)
    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )
    assert result.sample_count == 12
    assert result.direction == "rising"
    assert result.slope_usdt_per_minute == pytest.approx(1)


@pytest.mark.parametrize(
    "boundary", ["gap", "complete", "universe_changed", "null", "missing_minute"]
)
@pytest.mark.parametrize("remaining", [4, 5])
def test_regression_never_bridges_discontinuous_coverage(boundary, remaining):
    snapshot = cvd_snapshot(list(range(12)), now=NOW)
    index = len(snapshot.data) - remaining - 1
    if boundary == "null":
        snapshot.data[index].cvd = None
    elif boundary == "missing_minute":
        del snapshot.data[index]
    else:
        setattr(snapshot.data[index], boundary, boundary != "complete")
    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )

    assert result.sample_count == remaining
    assert result.direction == ("rising" if remaining == 5 else "unavailable")


@pytest.mark.parametrize(
    "case",
    [
        "stale_flag",
        "old_checkpoint",
        "missing_checkpoint",
        "future_checkpoint",
        "empty",
        "reversed",
        "duplicate",
        "stale_history",
        "future_bucket",
        "reset",
        "source",
    ],
)
def test_untrustworthy_or_missing_history_is_unavailable(case):
    snapshot = cvd_snapshot(list(range(12)), now=NOW)
    if case == "stale_flag":
        snapshot.stale = True
    elif case == "old_checkpoint":
        snapshot.updated_at = NOW - timedelta(seconds=91)
    elif case == "missing_checkpoint":
        snapshot.updated_at = None
    elif case == "future_checkpoint":
        snapshot.updated_at = NOW + timedelta(seconds=3)
    elif case == "empty":
        snapshot.data = []
    elif case == "reversed":
        snapshot.data.reverse()
    elif case == "duplicate":
        snapshot.data[-1].timestamp = snapshot.data[-2].timestamp
    elif case == "stale_history":
        for point in snapshot.data:
            point.timestamp -= timedelta(minutes=10)
        snapshot.reset_at -= timedelta(minutes=10)
    elif case == "future_bucket":
        snapshot.data[-1].timestamp = NOW + timedelta(minutes=1)
    elif case == "reset":
        snapshot.reset_at = snapshot.data[-1].timestamp + timedelta(seconds=1)
    else:
        snapshot.source = "another_exchange"

    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )
    assert result.direction == "unavailable"
    assert result.slope_usdt_per_minute is None
    assert result.pearson_r is None


def test_telegram_includes_the_same_direction_and_statistics_as_signal_evidence():
    snapshot = cvd_snapshot([1000, 850, 900, 700, 600, 500], now=NOW)
    result = calculate_cvd_direction(
        snapshot, interval_seconds=60, now_seconds=NOW.timestamp()
    )
    line = cvd_telegram_line("Symbol CVD", result)
    assert "Symbol CVD: falling" in line
    assert f"slope {result.slope_usdt_per_minute:+.2f} USDT/min" in line
    assert f"Pearson r {result.pearson_r:+.3f}" in line
    assert "threshold ±0.70" in line
    assert "6 completed 1m points" in line


def test_telegram_distinguishes_unavailable_data_from_measured_no_direction():
    snapshot = cvd_snapshot([], now=NOW)
    result = calculate_cvd_direction(
        snapshot, interval_seconds=900, now_seconds=NOW.timestamp()
    )
    assert (
        cvd_telegram_line("Market CVD", result)
        == "- Market CVD: unavailable (no observations)"
    )

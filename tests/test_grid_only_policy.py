from types import SimpleNamespace
from typing import cast

import pytest

from market_regime.grid_only_policy import GridOnlyPolicy
from market_regime.models import LiveMarketContext


def make_context(market_regime: str | None = "RANGE") -> LiveMarketContext:
    return cast(LiveMarketContext, SimpleNamespace(market_regime=market_regime))


def test_policy_activates_when_breadth_moves_toward_trend() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        {"market_breadth_ma": [0.12, 0.10]},
    )

    assert policy.allow_grid_ladder is True
    assert policy.block_standard_bots is True
    assert policy.direction == "toward_trend"
    assert policy.source == "market_breadth_ma"
    assert policy.previous == 0.10
    assert policy.latest == 0.12
    assert policy.momentum_points == pytest.approx(2.0)


def test_policy_activates_when_breadth_moves_toward_range() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context("TRANSITIONAL"),
        {"market_breadth_ma": [0.10, 0.12]},
    )

    assert policy.allow_grid_ladder is True
    assert policy.block_standard_bots is True
    assert policy.direction == "toward_range"
    assert policy.source == "market_breadth_ma"
    assert policy.previous == 0.12
    assert policy.latest == 0.10
    assert policy.momentum_points == pytest.approx(-2.0)


def test_policy_stays_inactive_for_flat_breadth() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        {"market_breadth_ma": [0.10, 0.10]},
    )

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "breadth_momentum_flat"


def test_policy_stays_inactive_for_missing_breadth() -> None:
    policy = GridOnlyPolicy.resolve(make_context(), {})

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "breadth_momentum_unavailable"


def test_policy_stays_inactive_for_missing_context() -> None:
    policy = GridOnlyPolicy.resolve(None, {"market_breadth_ma": [0.12, 0.10]})

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "market_context_unavailable"


def test_policy_orders_breadth_by_timestamp_when_available() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        {
            "timestamp": [
                "2026-07-04T00:15:00+00:00",
                "2026-07-04T00:00:00+00:00",
            ],
            "market_breadth_ma": [0.12, 0.10],
        },
    )

    assert policy.allow_grid_ladder is True
    assert policy.direction == "toward_trend"
    assert policy.previous == 0.10
    assert policy.latest == 0.12


def test_policy_falls_back_to_next_available_breadth_source() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        {
            "market_breadth_ma": ["not-a-number"],
            "market_breadth": [0.12, 0.10],
            "adp": [0.01, 0.02],
        },
    )

    assert policy.allow_grid_ladder is True
    assert policy.source == "market_breadth"
    assert policy.direction == "toward_trend"


@pytest.mark.parametrize("market_regime", ["TREND_UP", "TREND_DOWN", "HIGH_STRESS"])
def test_policy_excludes_non_grid_only_market_regimes(market_regime: str) -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(market_regime),
        {"market_breadth_ma": [0.12, 0.10]},
    )

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == f"market_regime_{market_regime.lower()}"

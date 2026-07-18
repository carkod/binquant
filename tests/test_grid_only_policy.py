from types import SimpleNamespace
from typing import cast

import pytest
from pybinbot import MarketBreadthSeries

from market_regime.grid_only_policy import GridOnlyPolicy
from market_regime.models import LiveMarketContext


def make_context(market_regime: str | None = "RANGE") -> LiveMarketContext:
    return cast(LiveMarketContext, SimpleNamespace(market_regime=market_regime))


def make_market_breadth_series(
    *,
    latest: float,
    previous: float,
    key: str = "market_breadth_ma",
) -> MarketBreadthSeries:
    return MarketBreadthSeries(
        timestamp=[
            "2026-07-04T00:15:00+00:00",
            "2026-07-04T00:00:00+00:00",
        ],
        advancers=[32, 30],
        decliners=[18, 20],
        market_breadth=[latest, previous] if key == "market_breadth" else [0.0, 0.0],
        market_breadth_ma=(
            [latest, previous] if key == "market_breadth_ma" else [None, None]
        ),
        avg_gain=[0.02, 0.01],
        avg_loss=[-0.01, -0.02],
        total_volume=[1000, 900],
        strength_index=[0.2, 0.1],
    )


def test_policy_activates_when_breadth_moves_toward_trend() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        make_market_breadth_series(latest=0.12, previous=0.10),
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
        make_market_breadth_series(latest=0.10, previous=0.12),
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
        make_market_breadth_series(latest=0.10, previous=0.10),
    )

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "breadth_momentum_flat"


def test_policy_stays_inactive_for_missing_breadth() -> None:
    policy = GridOnlyPolicy.resolve(make_context(), None)

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "breadth_momentum_unavailable"


def test_policy_stays_inactive_for_missing_context() -> None:
    policy = GridOnlyPolicy.resolve(
        None,
        make_market_breadth_series(latest=0.12, previous=0.10),
    )

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "market_context_unavailable"


def test_policy_orders_breadth_by_timestamp_when_available() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        make_market_breadth_series(latest=0.12, previous=0.10),
    )

    assert policy.allow_grid_ladder is True
    assert policy.direction == "toward_trend"
    assert policy.previous == 0.10
    assert policy.latest == 0.12


def test_policy_accepts_market_breadth_series_model() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        make_market_breadth_series(latest=0.12, previous=0.10),
    )

    assert policy.allow_grid_ladder is True
    assert policy.direction == "toward_trend"
    assert policy.source == "market_breadth_ma"


def test_policy_falls_back_to_next_available_breadth_source() -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(),
        make_market_breadth_series(
            latest=0.12,
            previous=0.10,
            key="market_breadth",
        ),
    )

    assert policy.allow_grid_ladder is True
    assert policy.source == "market_breadth"
    assert policy.direction == "toward_trend"


@pytest.mark.parametrize("market_regime", ["TREND_UP", "TREND_DOWN", "HIGH_STRESS"])
def test_policy_excludes_non_grid_only_market_regimes(market_regime: str) -> None:
    policy = GridOnlyPolicy.resolve(
        make_context(market_regime),
        make_market_breadth_series(latest=0.12, previous=0.10),
    )

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == f"market_regime_{market_regime.lower()}"

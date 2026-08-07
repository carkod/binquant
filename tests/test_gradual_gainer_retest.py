from unittest.mock import AsyncMock

from pandas import DataFrame
import pytest

from strategies.gradual_gainer_retest import (
    GradualGainerCandidate,
    GradualGainerPortfolioSelector,
    GradualGainerRetest,
)


def make_frames() -> tuple[DataFrame, DataFrame]:
    rows = []
    for index in range(120):
        close = 100.0 if index < 96 else 100.0 + (index - 96) * 0.5
        rows.append(
            {
                "open_time": 1_800_000_000_000 + index * 900_000,
                "close": close,
            }
        )
    symbol = DataFrame(rows)
    btc = DataFrame({"open_time": symbol["open_time"], "close": 100.0})
    return symbol, btc


def test_leadership_requires_positive_top_quantile_relative_strength():
    symbol, btc = make_frames()

    leader, rs_2h, rs_6h = GradualGainerRetest._leadership_allows(symbol, btc)

    assert leader is True
    assert rs_2h > 0
    assert rs_6h > 0


@pytest.mark.asyncio
async def test_portfolio_selector_dispatches_only_best_candidate_per_hour():
    selector = GradualGainerPortfolioSelector()
    first = AsyncMock()
    second = AsyncMock()
    next_hour = AsyncMock()
    hour = 1_800_000_000_000

    await selector.submit(GradualGainerCandidate(hour, "LOWUSDTM", 1.1, first))
    await selector.submit(
        GradualGainerCandidate(hour + 900_000, "HIGHUSDTM", 1.2, second)
    )
    await selector.submit(
        GradualGainerCandidate(hour + 3_600_000, "NEXTUSDTM", 1.0, next_hour)
    )

    first.assert_not_awaited()
    second.assert_awaited_once()
    next_hour.assert_not_awaited()

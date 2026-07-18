from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any, ClassVar

from pybinbot import MarketBreadthSeries, timestamp_sort_key

from market_regime.models import LiveMarketContext


@dataclass(frozen=True)
class GridOnlyPolicy:
    GRID_ONLY_REGIMES: ClassVar[frozenset[str]] = frozenset({"RANGE", "TRANSITIONAL"})
    BREADTH_SOURCES: ClassVar[tuple[tuple[str, bool], ...]] = (
        ("market_breadth_ma", True),
        ("market_breadth", True),
    )

    allow_grid_ladder: bool
    block_standard_bots: bool
    reason: str
    direction: str | None = None
    source: str | None = None
    latest: float | None = None
    previous: float | None = None
    momentum_points: float | None = None

    @classmethod
    def disabled(cls, reason: str) -> GridOnlyPolicy:
        return cls(
            allow_grid_ladder=False,
            block_standard_bots=False,
            reason=reason,
        )

    @classmethod
    def active(
        cls,
        *,
        direction: str,
        source: str,
        latest: float,
        previous: float,
    ) -> GridOnlyPolicy:
        return cls(
            allow_grid_ladder=True,
            block_standard_bots=True,
            reason=f"breadth_momentum_{direction}_{source}",
            direction=direction,
            source=source,
            latest=latest,
            previous=previous,
            momentum_points=(latest - previous) * 100,
        )

    @staticmethod
    def _coerce_breadth_value(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not isfinite(parsed):
            return None
        return parsed

    @classmethod
    def _ordered_breadth_values(
        cls,
        values: list[Any],
        timestamps: list[Any],
        *,
        newest_first: bool,
    ) -> list[float]:
        if len(values) >= 2 and len(timestamps) >= len(values):
            timestamped_values: list[tuple[float, float]] = []
            for timestamp, value in zip(timestamps, values, strict=False):
                sort_key = timestamp_sort_key(timestamp)
                breadth_value = cls._coerce_breadth_value(value)
                if sort_key is not None and breadth_value is not None:
                    timestamped_values.append((sort_key, breadth_value))

            if len(timestamped_values) >= 2:
                return [
                    breadth_value
                    for _, breadth_value in sorted(
                        timestamped_values, key=lambda item: item[0]
                    )
                ]

        parsed_values = [
            parsed
            for value in values
            if (parsed := cls._coerce_breadth_value(value)) is not None
        ]
        if newest_first:
            return list(reversed(parsed_values))
        return parsed_values

    @classmethod
    def _breadth_pair(
        cls,
        market_breadth_data: MarketBreadthSeries | None,
    ) -> tuple[float, float, str] | None:
        if market_breadth_data is None or len(market_breadth_data.timestamp) < 2:
            return None

        for source, newest_first in cls.BREADTH_SOURCES:
            values = getattr(market_breadth_data, source)

            breadth_values = cls._ordered_breadth_values(
                values=values,
                timestamps=market_breadth_data.timestamp,
                newest_first=newest_first,
            )
            if len(breadth_values) >= 2:
                return breadth_values[-2], breadth_values[-1], source

        return None

    @classmethod
    def resolve(
        cls,
        context: LiveMarketContext | None,
        market_breadth_data: MarketBreadthSeries | None,
    ) -> GridOnlyPolicy:
        if context is None:
            return GridOnlyPolicy.disabled("market_context_unavailable")

        market_regime = context.market_regime
        if market_regime is None:
            return GridOnlyPolicy.disabled("market_regime_unavailable")
        if market_regime not in cls.GRID_ONLY_REGIMES:
            return GridOnlyPolicy.disabled(f"market_regime_{market_regime.lower()}")

        breadth_pair = cls._breadth_pair(market_breadth_data)
        if breadth_pair is None:
            return GridOnlyPolicy.disabled("breadth_momentum_unavailable")

        previous, latest, source = breadth_pair
        latest_abs = abs(latest)
        previous_abs = abs(previous)
        if latest_abs > previous_abs:
            return GridOnlyPolicy.active(
                direction="toward_trend",
                source=source,
                latest=latest,
                previous=previous,
            )
        if latest_abs < previous_abs:
            return GridOnlyPolicy.active(
                direction="toward_range",
                source=source,
                latest=latest,
                previous=previous,
            )

        return GridOnlyPolicy.disabled("breadth_momentum_flat")

"""Calculate recorded CVD direction and format its signal evidence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from math import fsum, isfinite, sqrt
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pybinbot.models.cvd import CvdAggregateResponse


CVD_PEARSON_THRESHOLD = 0.70
CVD_MIN_POINTS = 5
CVD_MAX_POINTS = 12
CVD_CHECKPOINT_MAX_AGE_SECONDS = 90
CVD_LATE_ARRIVAL_SECONDS = 120


@dataclass(frozen=True)
class CvdDirection:
    direction: Literal["rising", "falling", "no_direction", "unavailable"]
    reason: str
    interval_seconds: int
    pearson_threshold: float = CVD_PEARSON_THRESHOLD
    slope_usdt_per_minute: float | None = None
    pearson_r: float | None = None
    sample_count: int = 0
    window_start: str | None = None
    window_end: str | None = None
    updated_at: str | None = None


def calculate_cvd_direction(
    snapshot: CvdAggregateResponse,
    *,
    interval_seconds: int,
    now_seconds: float,
    pearson_threshold: float = CVD_PEARSON_THRESHOLD,
) -> CvdDirection:
    """Fit the latest continuous completed segment, using at most 12 points.

    Symbol buckets span one minute; market buckets span 15 minutes. Both return
    slopes in USDT/minute and the same dimensionless Pearson score. Ignore
    trailing unfinished buckets, never bridge missing coverage or reset/contract
    changes, and distinguish unavailable evidence from a measured flat series.
    The 0.70 threshold describes linear consistency, not economic magnitude.
    """
    if interval_seconds <= 0 or not 0 < pearson_threshold <= 1:
        raise ValueError("Positive interval and Pearson threshold in (0, 1] required")

    evidence = CvdDirection(
        "unavailable",
        "insufficient_complete_points",
        interval_seconds,
        pearson_threshold=pearson_threshold,
        updated_at=(
            snapshot.updated_at.isoformat() if snapshot.updated_at is not None else None
        ),
    )
    if snapshot.source != "kucoin_futures":
        return replace(evidence, reason="unsupported_source")
    if (
        not isfinite(now_seconds)
        or snapshot.stale
        or snapshot.updated_at is None
        or not -2
        <= now_seconds - snapshot.updated_at.timestamp()
        <= CVD_CHECKPOINT_MAX_AGE_SECONDS
    ):
        return replace(evidence, reason="stale_checkpoint")
    if not snapshot.data:
        return replace(evidence, reason="no_observations")
    if any(
        later.timestamp <= earlier.timestamp
        for earlier, later in zip(snapshot.data, snapshot.data[1:], strict=False)
    ):
        return replace(evidence, reason="unordered_timestamps")

    points: list[tuple[float, float]] = []
    for point in reversed(snapshot.data):
        timestamp = point.timestamp.timestamp()
        if point.current or not point.complete:
            if points:
                break
            continue
        if (
            point.gap
            or point.universe_changed
            or point.cvd is None
            or timestamp < snapshot.reset_at.timestamp()
        ):
            break
        if timestamp + interval_seconds > now_seconds - CVD_LATE_ARRIVAL_SECONDS:
            return replace(evidence, reason="unfinalized_timestamp")
        if points and points[-1][0] - timestamp != interval_seconds:
            break
        points.append((timestamp, point.cvd))
        if len(points) == CVD_MAX_POINTS:
            break
    points.reverse()
    if points:
        evidence = replace(
            evidence,
            sample_count=len(points),
            window_start=datetime.fromtimestamp(points[0][0], UTC).isoformat(),
            window_end=datetime.fromtimestamp(points[-1][0], UTC).isoformat(),
        )
        if now_seconds - points[-1][0] > (
            2 * interval_seconds
            + CVD_LATE_ARRIVAL_SECONDS
            + CVD_CHECKPOINT_MAX_AGE_SECONDS
        ):
            return replace(evidence, reason="completed_history_stale")
    if len(points) < CVD_MIN_POINTS:
        return evidence

    x = [(timestamp - points[0][0]) / 60 for timestamp, _ in points]
    y = [value for _, value in points]
    mean_x, mean_y = fsum(x) / len(x), fsum(y) / len(y)
    xx = fsum((value - mean_x) ** 2 for value in x)
    yy = fsum((value - mean_y) ** 2 for value in y)
    xy = fsum(
        (timestamp - mean_x) * (value - mean_y)
        for timestamp, value in zip(x, y, strict=True)
    )
    slope = xy / xx
    r = max(-1.0, min(1.0, xy / sqrt(xx) / sqrt(yy))) if yy > 0 else 0.0
    direction: Literal["rising", "falling", "no_direction"] = "no_direction"
    if slope > 0 and r >= pearson_threshold:
        direction = "rising"
    elif slope < 0 and r <= -pearson_threshold:
        direction = "falling"
    return replace(
        evidence,
        direction=direction,
        reason="consistent_linear_direction"
        if direction != "no_direction"
        else "weak_linear_direction",
        slope_usdt_per_minute=slope,
        pearson_r=r,
    )


def cvd_telegram_line(label: str, result: CvdDirection) -> str:
    if result.direction == "unavailable":
        return f"- {label}: unavailable ({result.reason.replace('_', ' ')})"
    return (
        f"- {label}: {result.direction.replace('_', ' ')}"
        f" | slope {result.slope_usdt_per_minute:+.2f} USDT/min"
        f" | Pearson r {result.pearson_r:+.3f} (threshold ±{result.pearson_threshold:.2f})"
        f" | {result.sample_count} completed {result.interval_seconds // 60}m points"
    )

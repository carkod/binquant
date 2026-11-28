from collections.abc import Sequence
from math import fsum, isfinite


def regression_slope(samples: Sequence[tuple[float, float]]) -> float | None:
    """Fit all samples against elapsed hours; return value units per hour.

    Use the whole completed lookback, rather than just its two swing points.
    Signed values are supported. Flat data has zero slope; invalid or
    unordered observations cannot establish a trend.
    """
    if len(samples) < 3 or any(
        not isfinite(timestamp) or not isfinite(value) for timestamp, value in samples
    ):
        return None
    if any(
        later[0] <= earlier[0]
        for earlier, later in zip(samples, samples[1:], strict=False)
    ):
        return None
    origin_time, origin_value = samples[0]
    x = [(timestamp - origin_time) / 3600 for timestamp, _ in samples]
    y = [value - origin_value for _, value in samples]
    mean_x, mean_y = fsum(x) / len(x), fsum(y) / len(y)
    xx = fsum((value - mean_x) ** 2 for value in x)
    xy = fsum(
        (timestamp - mean_x) * (value - mean_y)
        for timestamp, value in zip(x, y, strict=True)
    )
    slope = xy / xx
    return slope if isfinite(slope) else None

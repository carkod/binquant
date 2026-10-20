from math import isfinite


def confirmed_higher_low(
    samples: list[tuple[float, float]],
    *,
    now_seconds: float,
    lookback_samples: int,
    fractal_wing: int,
    max_age_seconds: float,
    max_gap_seconds: float,
) -> dict[str, float] | None:
    """Compare the latest two strict, confirmed troughs in fresh continuous data.

    Values may be signed (market breadth) or positive (price lows). The newer
    trough must be higher and remain unbroken after its confirmation.
    """
    if len(samples) < lookback_samples or any(
        not isfinite(timestamp) for timestamp, _ in samples
    ):
        return None
    window = sorted(samples, key=lambda sample: sample[0])[-lookback_samples:]
    latest_timestamp, latest = window[-1]
    if not 0 <= now_seconds - latest_timestamp <= max_age_seconds:
        return None
    if any(not isfinite(value) for _, value in window):
        return None
    if any(
        not 0 < later[0] - earlier[0] <= max_gap_seconds
        for earlier, later in zip(window, window[1:], strict=False)
    ):
        return None

    lows = [
        i
        for i in range(fractal_wing, len(window) - fractal_wing)
        if all(
            window[i][1] < window[j][1]
            for j in range(i - fractal_wing, i + fractal_wing + 1)
            if i != j
        )
    ]
    if len(lows) < 2:
        return None
    earlier_pos, later_pos = lows[-2:]
    earlier_timestamp, earlier_low = window[earlier_pos]
    later_timestamp, later_low = window[later_pos]
    if later_low <= earlier_low or any(
        value <= later_low for _, value in window[later_pos + 1 :]
    ):
        return None
    return {
        "latest": latest,
        "timestamp": latest_timestamp,
        "higher_low_first_trough": earlier_low,
        "higher_low_second_trough": later_low,
        "higher_low_rise": later_low - earlier_low,
        "higher_low_first_timestamp": earlier_timestamp,
        "higher_low_second_timestamp": later_timestamp,
        "higher_low_confirmation_timestamp": window[later_pos + fractal_wing][0],
    }

from datetime import UTC, datetime, timedelta
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any


def cvd_snapshot(
    values: Sequence[float | None],
    *,
    now: datetime,
    interval_seconds: int = 60,
) -> Any:
    """A client-response test double; HTTP/model validation lives in pybinbot."""
    latest = (int(now.timestamp() - 120) // interval_seconds - 1) * interval_seconds
    start = datetime.fromtimestamp(latest - (len(values) - 1) * interval_seconds, UTC)
    return SimpleNamespace(
        source="kucoin_futures",
        symbol="TESTUSDTM",
        data=[
            SimpleNamespace(
                timestamp=start + timedelta(seconds=i * interval_seconds),
                cvd=value,
                complete=True,
                gap=False,
                current=False,
                universe_changed=False,
            )
            for i, value in enumerate(values)
        ],
        reset_at=start,
        updated_at=now,
        stale=False,
        partial=False,
    )

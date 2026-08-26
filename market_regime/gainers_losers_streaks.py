from pybinbot import GainersLosersSnapshot
from pydantic import BaseModel, ConfigDict


class TopGainerStreak(BaseModel):
    """
    How long a symbol has held its place on the top-gainers tape.

    A streak counts only the current, unbroken run of snapshots. A coin that
    dropped off the list and came back scores its latest run, not its total
    number of appearances, so a serial one-off spiker cannot accumulate a
    streak it never actually sustained.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshots_in_a_row: int
    latest_price_change_percent: float


def resolve_top_gainer_streak(
    snapshots: list[GainersLosersSnapshot],
    symbol: str,
) -> TopGainerStreak:
    """
    Count the unbroken run of newest-first snapshots listing `symbol` as a
    top gainer.

    binbot returns snapshots newest-first, so the run is read from the front
    and stops at the first snapshot that omits the symbol.
    """
    snapshots_in_a_row = 0
    latest_price_change_percent = 0.0

    for snapshot in snapshots:
        entry = next(
            (item for item in snapshot.top_gainers if item.symbol == symbol),
            None,
        )
        if entry is None:
            break
        if snapshots_in_a_row == 0:
            latest_price_change_percent = entry.price_change_percent
        snapshots_in_a_row += 1

    return TopGainerStreak(
        snapshots_in_a_row=snapshots_in_a_row,
        latest_price_change_percent=latest_price_change_percent,
    )

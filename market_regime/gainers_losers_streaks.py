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


class TopLoserStreak(BaseModel):
    """Current, unbroken presence on the top-losers tape."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshots_in_a_row: int
    latest_rank: int | None
    latest_price_change_percent: float
    rank_change: int | None
    price_change_percent_change: float | None


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


def resolve_top_loser_streak(
    snapshots: list[GainersLosersSnapshot],
    symbol: str,
) -> TopLoserStreak:
    """
    Resolve the current newest-first top-loser run and its one-snapshot move.

    A negative ``price_change_percent_change`` means the 24-hour loss worsened
    since the preceding snapshot. A negative ``rank_change`` means the symbol
    moved closer to the number-one loser position.
    """
    ranked_entries: list[tuple[int, float]] = []

    for snapshot in snapshots:
        ranked_entry = next(
            (
                (rank, item.price_change_percent)
                for rank, item in enumerate(snapshot.top_losers, start=1)
                if item.symbol == symbol
            ),
            None,
        )
        if ranked_entry is None:
            break
        ranked_entries.append(ranked_entry)

    if not ranked_entries:
        return TopLoserStreak(
            snapshots_in_a_row=0,
            latest_rank=None,
            latest_price_change_percent=0.0,
            rank_change=None,
            price_change_percent_change=None,
        )

    latest_rank, latest_price_change_percent = ranked_entries[0]
    previous_entry = ranked_entries[1] if len(ranked_entries) > 1 else None

    return TopLoserStreak(
        snapshots_in_a_row=len(ranked_entries),
        latest_rank=latest_rank,
        latest_price_change_percent=latest_price_change_percent,
        rank_change=(
            latest_rank - previous_entry[0] if previous_entry is not None else None
        ),
        price_change_percent_change=(
            latest_price_change_percent - previous_entry[1]
            if previous_entry is not None
            else None
        ),
    )

from pybinbot import GainerLoserEntry, GainersLosersSnapshot

from market_regime.gainers_losers_streaks import (
    resolve_top_gainer_streak,
    resolve_top_loser_streak,
)


def make_snapshot(*gainers: tuple[str, float]) -> GainersLosersSnapshot:
    return GainersLosersSnapshot(
        source="kucoin_futures",
        recorded_at="2026-08-26T11:11:34.771019+01:00",
        top_gainers=[
            GainerLoserEntry(symbol=symbol, price_change_percent=pct)
            for symbol, pct in gainers
        ],
        top_losers=[],
    )


def test_streak_counts_unbroken_run_from_newest_snapshot() -> None:
    snapshots = [
        make_snapshot(("BTRUSDTM", 181.88)),
        make_snapshot(("BTRUSDTM", 177.02)),
        make_snapshot(("BTRUSDTM", 142.81)),
        make_snapshot(("BMTUSDTM", 44.62)),
    ]

    streak = resolve_top_gainer_streak(snapshots=snapshots, symbol="BTRUSDTM")

    assert streak.snapshots_in_a_row == 3
    assert streak.latest_price_change_percent == 181.88


def test_streak_stops_at_first_snapshot_without_the_symbol() -> None:
    """
    A coin that dropped off the tape and came back scores only its current
    run, so an intermittent one-off spiker cannot accumulate a streak it
    never sustained.
    """
    snapshots = [
        make_snapshot(("STARUSDTM", 41.57)),
        make_snapshot(("BMTUSDTM", 44.62)),
        make_snapshot(("STARUSDTM", 40.44)),
        make_snapshot(("STARUSDTM", 29.18)),
    ]

    streak = resolve_top_gainer_streak(snapshots=snapshots, symbol="STARUSDTM")

    assert streak.snapshots_in_a_row == 1
    assert streak.latest_price_change_percent == 41.57


def test_streak_is_zero_when_symbol_never_appears() -> None:
    snapshots = [make_snapshot(("BTRUSDTM", 181.88))]

    streak = resolve_top_gainer_streak(snapshots=snapshots, symbol="VELVETUSDTM")

    assert streak.snapshots_in_a_row == 0
    assert streak.latest_price_change_percent == 0.0


def test_streak_is_zero_without_any_snapshots() -> None:
    streak = resolve_top_gainer_streak(snapshots=[], symbol="BTRUSDTM")

    assert streak.snapshots_in_a_row == 0
    assert streak.latest_price_change_percent == 0.0


def test_top_loser_streak_includes_latest_rank_and_hourly_change() -> None:
    snapshots = [
        GainersLosersSnapshot(
            source="kucoin_futures",
            recorded_at="2026-08-26T12:00:00+00:00",
            top_gainers=[],
            top_losers=[
                GainerLoserEntry(symbol="OTHERUSDTM", price_change_percent=-20.0),
                GainerLoserEntry(symbol="TESTUSDTM", price_change_percent=-18.0),
            ],
        ),
        GainersLosersSnapshot(
            source="kucoin_futures",
            recorded_at="2026-08-26T11:00:00+00:00",
            top_gainers=[],
            top_losers=[
                GainerLoserEntry(symbol="OTHERUSDTM", price_change_percent=-17.0),
                GainerLoserEntry(symbol="THIRDUSDTM", price_change_percent=-15.0),
                GainerLoserEntry(symbol="TESTUSDTM", price_change_percent=-14.0),
            ],
        ),
    ]

    streak = resolve_top_loser_streak(snapshots=snapshots, symbol="TESTUSDTM")

    assert streak.snapshots_in_a_row == 2
    assert streak.latest_rank == 2
    assert streak.latest_price_change_percent == -18.0
    assert streak.rank_change == -1
    assert streak.price_change_percent_change == -4.0


def test_top_loser_streak_is_zero_when_symbol_is_absent() -> None:
    streak = resolve_top_loser_streak(
        snapshots=[make_snapshot(("TESTUSDTM", 12.0))],
        symbol="TESTUSDTM",
    )

    assert streak.snapshots_in_a_row == 0
    assert streak.latest_rank is None
    assert streak.latest_price_change_percent == 0.0
    assert streak.rank_change is None
    assert streak.price_change_percent_change is None

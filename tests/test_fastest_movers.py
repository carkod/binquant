from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pybinbot import FastestMoverEntry, FastestMoversSnapshot

import shared.fastest_movers as fastest_movers
from shared.config import Config
from shared.fastest_movers import FastestMoversSignal, next_fastest_movers_check
from shared.telegram_signal_notifier import TelegramSignalNotifier

NOW = datetime(2026, 11, 27, 12, 32, tzinfo=UTC)
LATEST = NOW.replace(minute=30)
PREVIOUS = LATEST - timedelta(minutes=30)


def snapshot(
    at: datetime,
    gainers: tuple[str, ...] = (),
    losers: tuple[str, ...] = (),
) -> FastestMoversSnapshot:
    def entry(symbol: str, change: float) -> FastestMoverEntry:
        return FastestMoverEntry(
            symbol=symbol,
            price_change_1h_percent=change,
            price_change_3h_percent=None,
            price_change_percent=change * 3,
            last_price=100,
            turnover_24h=100000,
        )

    return FastestMoversSnapshot(
        source="kucoin_futures",
        recorded_at=at,
        comparison_at=at - timedelta(hours=1),
        gainers=[entry(symbol, 5) for symbol in gainers],
        losers=[entry(symbol, -5) for symbol in losers],
    )


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    current = {"now": NOW}
    fake_datetime = Mock()
    fake_datetime.now.side_effect = lambda tz: current["now"]
    monkeypatch.setattr(fastest_movers, "datetime", fake_datetime)
    return current


def watcher(path: Path, *snapshots: FastestMoversSnapshot) -> Any:
    api = Mock()
    api.get_fastest_movers = AsyncMock(side_effect=snapshots)
    notifier = Mock(spec=TelegramSignalNotifier)
    notifier.dispatch_signal = AsyncMock(return_value=True)
    return FastestMoversSignal(
        binbot_api=api,
        telegram_notifier=notifier,
        config=cast(
            Config,
            SimpleNamespace(env="test", backend_domain="https://api.example.com"),
        ),
        state_path=path,
    )


@pytest.mark.asyncio
async def test_first_valid_snapshot_is_a_quiet_baseline(tmp_path):
    signal = watcher(tmp_path / "state.json", snapshot(LATEST, ("SOL",), ("BTC",)))
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()
    assert signal.state.gainers == {"SOL"}


@pytest.mark.asyncio
async def test_unchanged_membership_and_rank_changes_do_not_notify(tmp_path):
    signal = watcher(
        tmp_path / "state.json",
        snapshot(PREVIOUS, ("SOL", "ETH"), ("BTC",)),
        snapshot(LATEST, ("ETH", "SOL"), ("BTC",)),
    )
    await signal.signal()
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_new_members_are_notified_with_snapshot_expiry(tmp_path):
    signal = watcher(
        tmp_path / "state.json",
        snapshot(PREVIOUS, ("SOL",), ("BTC",)),
        snapshot(LATEST, ("SOL", "ETH"), ("BTC", "DOGE")),
    )
    await signal.signal()
    await signal.signal()
    calls = signal.telegram_notifier.dispatch_signal.await_args_list
    assert len(calls) == 2
    assert "#ETH" in calls[0].args[0]
    assert "Event: entered Rising" in calls[0].args[0]
    assert "Rank: 2" in calls[0].args[0]
    assert "3h price change: unavailable" in calls[0].args[0]
    assert "#DOGE" in calls[1].args[0]
    assert "Event: entered Falling" in calls[1].args[0]
    assert calls[0].kwargs == {"expires_at": LATEST + timedelta(minutes=15)}


@pytest.mark.asyncio
async def test_side_switch_is_a_new_entry(tmp_path):
    signal = watcher(
        tmp_path / "state.json",
        snapshot(PREVIOUS, ("SOL",)),
        snapshot(LATEST, losers=("SOL",)),
    )
    await signal.signal()
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_awaited_once()
    assert (
        "entered Falling" in signal.telegram_notifier.dispatch_signal.await_args.args[0]
    )


@pytest.mark.asyncio
async def test_exit_and_reentry_notifies_again(tmp_path, clock):
    signal = watcher(
        tmp_path / "state.json",
        snapshot(PREVIOUS, ("SOL",)),
        snapshot(LATEST),
        snapshot(LATEST + timedelta(minutes=30), ("SOL",)),
    )
    await signal.signal()
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()
    clock["now"] += timedelta(minutes=30)
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_awaited_once()


@pytest.mark.asyncio
async def test_processed_snapshot_is_not_replayed_after_restart(tmp_path):
    path = tmp_path / "state.json"
    latest = snapshot(LATEST, ("ETH",))
    signal = watcher(path, snapshot(PREVIOUS, ("SOL",)), latest)
    await signal.signal()
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_awaited_once()
    restarted = watcher(path, latest)
    await restarted.signal()
    restarted.telegram_notifier.dispatch_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_delivery_is_attempted_once_and_never_replayed(tmp_path):
    latest = snapshot(LATEST, ("ETH",))
    path = tmp_path / "state.json"
    signal = watcher(path, snapshot(PREVIOUS), latest, latest)
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.return_value = False
    await signal.signal()
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_awaited_once()
    restarted = watcher(path, latest)
    await restarted.signal()
    restarted.telegram_notifier.dispatch_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_alert_is_dropped_at_fifteen_minutes_old(tmp_path, clock):
    signal = watcher(
        tmp_path / "state.json", snapshot(PREVIOUS), snapshot(LATEST, ("ETH",))
    )
    await signal.signal()
    clock["now"] = LATEST + timedelta(minutes=15)
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()
    assert signal.state.gainers == {"ETH"}


@pytest.mark.asyncio
async def test_missed_snapshot_establishes_a_quiet_baseline(tmp_path):
    signal = watcher(
        tmp_path / "state.json",
        snapshot(LATEST - timedelta(hours=1)),
        snapshot(LATEST, ("ETH",)),
    )
    await signal.signal()
    await signal.signal()
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_incomplete_history_does_not_replace_membership(tmp_path):
    incomplete = snapshot(LATEST)
    incomplete.comparison_at = None
    signal = watcher(tmp_path / "state.json", snapshot(PREVIOUS, ("SOL",)), incomplete)
    await signal.signal()
    await signal.signal()
    assert signal.state.gainers == {"SOL"}
    assert signal.state.recorded_at == PREVIOUS


@pytest.mark.asyncio
async def test_older_snapshot_cannot_replace_checkpoint(tmp_path):
    signal = watcher(
        tmp_path / "state.json",
        snapshot(LATEST, ("SOL",)),
        snapshot(PREVIOUS, ("ETH",)),
    )
    await signal.signal()
    await signal.signal()
    assert signal.state.gainers == {"SOL"}
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_failure_preserves_membership_without_notification(tmp_path):
    signal = watcher(tmp_path / "state.json", snapshot(PREVIOUS, ("SOL",)))
    await signal.signal()
    signal.binbot_api.get_fastest_movers.side_effect = RuntimeError("unavailable")
    with pytest.raises(RuntimeError, match="unavailable"):
        await signal.signal()
    assert signal.state.gainers == {"SOL"}
    signal.telegram_notifier.dispatch_signal.assert_not_awaited()


@pytest.mark.parametrize(
    "hour,minute,second,expected_hour,expected_minute",
    [
        (12, 0, 0, 12, 2),
        (12, 2, 0, 12, 32),
        (12, 31, 59, 12, 32),
        (12, 32, 0, 13, 2),
        (23, 59, 0, 0, 2),
    ],
)
def test_checks_align_with_fixed_half_hour_ingestion(
    hour, minute, second, expected_hour, expected_minute
):
    now = NOW.replace(hour=hour, minute=minute, second=second)
    next_check = next_fastest_movers_check(now)
    assert next_check.hour == expected_hour
    assert next_check.minute == expected_minute
    assert next_check.second == 0
    assert next_check > now


def test_check_schedule_uses_utc_for_other_local_timezones():
    from datetime import timezone

    now = datetime(
        2026, 11, 27, 12, 31, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    assert next_fastest_movers_check(now) == datetime(2026, 11, 27, 7, 2, tzinfo=UTC)

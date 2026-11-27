"""Notification-only entries into the dashboard's one-hour futures rankings."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, ValidationError
from pybinbot import BinbotApi

from shared.config import Config
from shared.telegram_signal_notifier import TelegramSignalNotifier


class FastestMoversState(BaseModel):
    backend_domain: str
    source: str
    recorded_at: AwareDatetime
    gainers: set[str]
    losers: set[str]


def next_fastest_movers_check(now: datetime) -> datetime:
    """Use fixed UTC :02/:32 checks, two minutes after each ingestion slot."""
    now = now.astimezone(UTC)
    hour = now.replace(minute=0, second=0, microsecond=0)
    for minutes in (2, 32, 62):
        candidate = hour + timedelta(minutes=minutes)
        if candidate > now:
            return candidate
    raise AssertionError("A future half-hour check must exist")


class FastestMoversSignal:
    ALGO = "fastest_movers"
    MAX_AGE = timedelta(minutes=15)
    # A missing half-hour snapshot makes the time of membership entry unknown.
    MAX_SNAPSHOT_GAP = timedelta(minutes=45)

    def __init__(
        self,
        *,
        binbot_api: BinbotApi,
        telegram_notifier: TelegramSignalNotifier,
        config: Config,
        state_path: Path,
    ) -> None:
        self.binbot_api = binbot_api
        self.telegram_notifier = telegram_notifier
        self.config = config
        self.state_path = state_path
        self.state: FastestMoversState | None = None
        try:
            state = FastestMoversState.model_validate_json(state_path.read_text())
            if state.backend_domain == config.backend_domain:
                self.state = state
        except FileNotFoundError:
            pass
        except OSError, ValidationError:
            logging.warning(
                "Fastest-movers checkpoint unavailable; starting with a quiet baseline"
            )

    async def signal(self) -> None:
        snapshot = await self.binbot_api.get_fastest_movers(limit=10)
        now = datetime.now(UTC)
        recorded_at = snapshot.recorded_at
        if recorded_at is None or snapshot.comparison_at is None or recorded_at > now:
            return
        previous = self.state
        if previous is not None and recorded_at <= previous.recorded_at:
            return

        current = FastestMoversState(
            backend_domain=self.config.backend_domain,
            source=snapshot.source,
            recorded_at=recorded_at,
            gainers={mover.symbol for mover in snapshot.gainers},
            losers={mover.symbol for mover in snapshot.losers},
        )
        # Checkpoint before attempting delivery: failures and restarts never replay alerts.
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.state_path.with_suffix(".tmp")
        temporary_path.write_text(current.model_dump_json(), encoding="utf-8")
        temporary_path.replace(self.state_path)
        self.state = current

        expires_at = recorded_at + self.MAX_AGE
        if (
            previous is None
            or previous.source != current.source
            or recorded_at - previous.recorded_at > self.MAX_SNAPSHOT_GAP
            or now >= expires_at
        ):
            return

        for side, movers, previous_members in (
            ("Rising", snapshot.gainers, previous.gainers),
            ("Falling", snapshot.losers, previous.losers),
        ):
            for rank, mover in enumerate(movers, start=1):
                if mover.symbol in previous_members:
                    continue
                change_3h = (
                    f"{mover.price_change_3h_percent:+.2f}%"
                    if mover.price_change_3h_percent is not None
                    else "unavailable"
                )
                message = f"""
                    - [{self.config.env}] <strong>#{self.ALGO} algorithm</strong> #{mover.symbol}
                    - Event: entered {side}
                    - Event ID: {recorded_at.isoformat()}
                    - Rank: {rank}
                    - 1h price change: {mover.price_change_1h_percent:+.2f}%
                    - 3h price change: {change_3h}
                    - 24h price change: {mover.price_change_percent:+.2f}%
                    - Current price: {mover.last_price}
                    - Snapshot time: {recorded_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}
                    - Autotrade: disabled, notification only
                """
                await self.telegram_notifier.dispatch_signal(
                    message, expires_at=expires_at
                )

    async def run_forever(self) -> None:
        """Bootstrap membership, then check on the fixed half-hour schedule."""
        while True:
            try:
                await self.signal()
            except Exception:
                logging.exception("Fastest-movers check failed; skipping this slot")
            now = datetime.now(UTC)
            await asyncio.sleep((next_fastest_movers_check(now) - now).total_seconds())

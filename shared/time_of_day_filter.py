"""
Time-of-day autotrade filter for chop-prone perpetual strategies.

Background
----------
Production analysis (2026-04-22..29) showed bot opens between 20:00-22:00 LON
won 1 of 13 (8% win rate) versus mid-day RANGE entries that won 33%. The
20:00-22:00 LON window is the end of the NYC session and the start of the
Asia session, when liquidity on KuCoin micro-cap perpetuals is thinnest and
spreads widen, which routinely prints stop-loss-sized noise bars.

Filter
------
Block autotrade activation when local London time is in `QUIET_HOURS` UNLESS
the regime is strongly trending and stable (the only condition under which
late-day continuation moves are reliable enough to overcome the spread).

When the filter blocks, callers should:
  1. Force the SignalsConsumer to `autotrade=False` (keep the alert; skip the bot)
  2. Send a `#time_of_day_block` notification via TelegramConsumer

The notification format mirrors market_regime_notifier.py — leading metadata, structured
key/value lines — so existing Telegram parsing and dashboards can ingest it.
"""

from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from market_regime.models import LiveMarketContext


LONDON = ZoneInfo("Europe/London")

# London-local hours (24h) when autotrade activation is suppressed for
# chop-prone strategies. Inclusive of QUIET_START_HOUR, exclusive of
# QUIET_END_HOUR.
QUIET_START_HOUR = 20
QUIET_END_HOUR = 23

# Trending markets get a pass during quiet hours only when the move is
# strong enough that thin liquidity is unlikely to whipsaw the entry.
_OVERRIDE_REGIMES = {"TREND_UP", "TREND_DOWN"}
_MIN_TRANSITION_STRENGTH = 0.7


def _now_london(now: datetime | None = None) -> datetime:
    if now is None:
        now = datetime.now(tz=LONDON)
    return now.astimezone(LONDON)


def is_quiet_hours(now: datetime | None = None) -> bool:
    """True when the current London-local hour is within the suppressed window."""
    return QUIET_START_HOUR <= _now_london(now).hour < QUIET_END_HOUR


def is_autotrade_suppressed(
    context: LiveMarketContext | None,
    now: datetime | None = None,
) -> bool:
    """
    Suppress autotrade when in quiet hours, with a narrow override for
    strong, stable trending regimes.
    """
    if not is_quiet_hours(now):
        return False
    if context is None:
        return True
    if context.market_regime in _OVERRIDE_REGIMES and (
        context.market_regime_transition_strength >= _MIN_TRANSITION_STRENGTH
    ):
        return False
    return True


def build_quiet_hours_signal_msg(
    symbol: str,
    algo: str,
    side: str,
    context: LiveMarketContext | None,
    now: datetime | None = None,
) -> str:
    """
    Render the Telegram alert for a suppressed activation. Keeps the
    line/key shape used by market_regime_notifier.py so downstream parsers stay uniform.
    """
    london_now = _now_london(now)
    regime = context.market_regime if context is not None else "UNAVAILABLE"
    transition = (
        context.market_regime_transition
        if context is not None and context.market_regime_transition is not None
        else "None"
    )
    transition_strength = (
        f"{context.market_regime_transition_strength:.3f}"
        if context is not None
        else "n/a"
    )
    stress = f"{context.market_stress_score:.3f}" if context is not None else "n/a"
    return f"""
        - [{os.getenv("ENV", "")}] <strong>#time_of_day_block</strong>
        - Symbol: {symbol}
        - Algorithm: {algo}
        - Side: {side}
        - Reason: London time {london_now.strftime("%H:%M")} falls in the {QUIET_START_HOUR:02d}:00-{QUIET_END_HOUR:02d}:00 quiet window
        - Market regime: {regime}
        - Market transition: {transition}
        - Transition strength: {transition_strength}
        - Market stress: {stress}
        - Action: autotrade suppressed (signal kept as alert only)
    """

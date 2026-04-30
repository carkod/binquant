from datetime import datetime

from shared.time_of_day_filter import (
    LONDON,
    QUIET_END_HOUR,
    QUIET_START_HOUR,
    build_quiet_hours_signal_msg,
    is_autotrade_suppressed,
    is_quiet_hours,
)
from market_regime.models import LiveMarketContext

# Tests in this file import the filter symbols directly, so the local refs
# bind to the real implementation at import time — the conftest autouse fixture
# patches the module attribute on shared.time_of_day_filter, not these locals,
# so no per-module restoration is needed.


def _make_context(**overrides) -> LiveMarketContext:
    base = {
        "timestamp": 1_700_000_000_000,
        "fresh_count": 200,
        "total_tracked_symbols": 200,
        "coverage_ratio": 1.0,
        "btc_symbol": "BTCUSDT",
        "btc_present": True,
        "confidence": 1.0,
        "is_provisional": False,
        "advancers": 100,
        "decliners": 100,
        "advancers_ratio": 0.5,
        "decliners_ratio": 0.5,
        "advancers_decliners_ratio": 1.0,
        "average_return": 0.0,
        "average_relative_strength_vs_btc": 0.0,
        "pct_above_ema20": 0.5,
        "pct_above_ema50": 0.5,
        "average_trend_score": 0.0,
        "average_atr_pct": 0.02,
        "average_bb_width": 0.04,
        "btc_return": 0.0,
        "btc_trend_score": 0.0,
        "btc_regime_score": 0.0,
        "market_stress_score": 0.0,
        "long_tailwind": 0.0,
        "short_tailwind": 0.0,
        "market_regime": "RANGE",
        "previous_market_regime": "RANGE",
        "market_regime_transition": None,
        "market_regime_transition_strength": 0.0,
        "long_regime_score": 0.0,
        "short_regime_score": 0.0,
        "range_regime_score": 0.5,
        "stress_regime_score": 0.0,
        "regime_is_transitioning": False,
        "regime_stable_since": 1_699_999_000_000,
        "symbol_features": {},
        "metadata": {},
    }
    base.update(overrides)
    return LiveMarketContext(**base)


def _at_london(hour: int) -> datetime:
    return datetime(2026, 4, 29, hour, 0, tzinfo=LONDON)


def test_quiet_hours_window_inclusive_start_exclusive_end():
    assert is_quiet_hours(_at_london(QUIET_START_HOUR)) is True
    assert is_quiet_hours(_at_london(QUIET_END_HOUR - 1)) is True
    assert is_quiet_hours(_at_london(QUIET_END_HOUR)) is False
    assert is_quiet_hours(_at_london(QUIET_START_HOUR - 1)) is False
    assert is_quiet_hours(_at_london(12)) is False


def test_suppressed_inside_quiet_hours_for_range_regime():
    ctx = _make_context(market_regime="RANGE")
    assert is_autotrade_suppressed(ctx, now=_at_london(21)) is True


def test_strong_trend_overrides_quiet_hours():
    ctx = _make_context(
        market_regime="TREND_UP",
        market_regime_transition_strength=0.85,
    )
    assert is_autotrade_suppressed(ctx, now=_at_london(21)) is False


def test_weak_trend_does_not_override_quiet_hours():
    ctx = _make_context(
        market_regime="TREND_UP",
        market_regime_transition_strength=0.2,
    )
    assert is_autotrade_suppressed(ctx, now=_at_london(21)) is True


def test_outside_quiet_hours_never_suppressed():
    ctx = _make_context(market_regime="RANGE")
    assert is_autotrade_suppressed(ctx, now=_at_london(13)) is False


def test_message_includes_required_signal_tags():
    ctx = _make_context(market_regime="RANGE")
    msg = build_quiet_hours_signal_msg(
        symbol="MOVEUSDTM",
        algo="coinrule_price_tracker",
        side="long",
        context=ctx,
        now=_at_london(21),
    )
    assert "#time_of_day_block" in msg
    assert "MOVEUSDTM" in msg
    assert "coinrule_price_tracker" in msg
    assert "21:00" in msg


def test_no_context_in_quiet_hours_is_suppressed():
    assert is_autotrade_suppressed(None, now=_at_london(21)) is True

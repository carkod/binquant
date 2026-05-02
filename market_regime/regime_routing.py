from __future__ import annotations

from market_regime.models import LiveMarketContext, SymbolMarketFeatures


# Default minimum age (ms) for a regime to be considered stable enough to
# autotrade against. Five days of production data showed regime flips every
# ~4.5h on average; 30 minutes filters the worst stale-label opens without
# starving normal regime-aligned entries.
DEFAULT_REGIME_STABILITY_MS = 30 * 60 * 1000


def resolve_symbol_features(
    context: LiveMarketContext | None,
    symbol: str,
) -> SymbolMarketFeatures | None:
    if context is None:
        return None
    return context.get_symbol_features(symbol)


def regime_age_ms(context: LiveMarketContext | None) -> int | None:
    """How long (ms) the current market_regime has been in place, or None
    when the context has no stability anchor yet."""
    if context is None or context.regime_stable_since is None:
        return None
    return max(context.timestamp - context.regime_stable_since, 0)


def is_regime_stable(
    context: LiveMarketContext | None,
    min_age_ms: int = DEFAULT_REGIME_STABILITY_MS,
) -> bool:
    """True when the current market regime has held steady for at least
    `min_age_ms` and no in-flight transition is flagged."""
    if context is None:
        return False
    if context.regime_is_transitioning:
        return False
    age = regime_age_ms(context)
    if age is None:
        # First context after restart — we can't claim stability yet.
        return False
    return age >= min_age_ms


def allows_long_autotrade(
    context: LiveMarketContext | None,
    symbol: str | None = None,
) -> bool:
    if context is None:
        return False
    if context.regime_is_transitioning:
        return False
    if not is_regime_stable(context):
        return False

    if context.market_regime in {"HIGH_STRESS", "TREND_DOWN", "TRANSITIONAL"}:
        return False

    if context.market_stress_score >= 0.35:
        return False

    if symbol is None:
        return context.market_regime in {"TREND_UP", "RANGE"}

    features = resolve_symbol_features(context, symbol)
    if features is None:
        return context.market_regime in {"TREND_UP", "RANGE"}

    if features.micro_regime == "TREND_DOWN":
        return features.micro_regime_transition == "RECOVERY"
    if features.micro_regime == "VOLATILE":
        return False
    return features.micro_regime in {"TREND_UP", "RANGE", "TRANSITIONAL"}

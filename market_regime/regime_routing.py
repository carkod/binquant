from __future__ import annotations

from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def resolve_symbol_features(
    context: LiveMarketContext | None,
    symbol: str,
) -> SymbolMarketFeatures | None:
    if context is None:
        return None
    return context.get_symbol_features(symbol)


def allows_long_autotrade(
    context: LiveMarketContext | None,
    symbol: str | None = None,
) -> bool:
    if context is None:
        return False
    if context.regime_is_transitioning:
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


def supports_grid_trading(
    context: LiveMarketContext | None,
    symbol: str | None = None,
) -> bool:
    if context is None:
        return True
    if context.regime_is_transitioning:
        return False

    if context.market_regime not in {"RANGE", "TRANSITIONAL"}:
        return False
    if context.market_stress_score >= 0.35:
        return False

    features = resolve_symbol_features(context, symbol) if symbol is not None else None
    if features is None:
        return True

    if features.micro_regime == "VOLATILE":
        return False
    if features.micro_regime_transition in {"BREAKDOWN", "BREAKOUT_UP"}:
        return False
    return features.micro_regime in {"RANGE", "TRANSITIONAL"}

from __future__ import annotations

from typing import Any, cast

from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def resolve_symbol_features(
    context: LiveMarketContext | object | None,
    symbol: str,
) -> SymbolMarketFeatures | None:
    if context is None:
        return None

    getter = getattr(context, "get_symbol_features", None)
    if callable(getter):
        return cast(SymbolMarketFeatures | None, getter(symbol))

    raw_symbol_features = getattr(context, "symbol_features", None)
    if not isinstance(raw_symbol_features, dict):
        return None

    symbol_features = cast(dict[str, Any], raw_symbol_features)
    normalized = symbol.strip().upper()
    direct = symbol_features.get(normalized)
    if isinstance(direct, SymbolMarketFeatures):
        return direct

    canonical = normalized.replace("-", "").replace("_", "")
    for known_symbol, features in symbol_features.items():
        if (
            isinstance(known_symbol, str)
            and known_symbol.upper().replace("-", "").replace("_", "") == canonical
            and isinstance(features, SymbolMarketFeatures)
        ):
            return features
    return None


def allows_long_autotrade(
    context: LiveMarketContext | None,
    symbol: str | None = None,
) -> bool:
    if context is None:
        return False

    regime = getattr(context, "market_regime", "UNDEFINED")
    if regime in {"UNDEFINED", None}:
        if getattr(context, "market_stress_score", 1.0) >= 0.35:
            return False
        return getattr(context, "advancers_ratio", 0.5) >= 0.55

    if regime in {"HIGH_STRESS", "TREND_DOWN", "TRANSITIONAL"}:
        return False

    if getattr(context, "market_stress_score", 1.0) >= 0.35:
        return False

    if symbol is None:
        return regime in {"TREND_UP", "RANGE"}

    features = resolve_symbol_features(context, symbol)
    if features is None:
        return regime in {"TREND_UP", "RANGE"}

    micro_regime = getattr(features, "micro_regime", "UNDEFINED")
    micro_transition = getattr(features, "micro_regime_transition", None)
    if micro_regime == "UNDEFINED":
        return regime in {"TREND_UP", "RANGE"}
    if micro_regime == "TREND_DOWN":
        return micro_transition == "RECOVERY"
    if micro_regime == "VOLATILE":
        return False
    return micro_regime in {"TREND_UP", "RANGE", "TRANSITIONAL"}


def allows_short_autotrade(
    context: LiveMarketContext | None,
    symbol: str | None = None,
) -> bool:
    if context is None:
        return False

    regime = getattr(context, "market_regime", "UNDEFINED")
    if regime in {"UNDEFINED", None}:
        if getattr(context, "market_stress_score", 0.0) >= 0.35:
            return False
        return getattr(context, "advancers_ratio", 0.5) <= 0.45

    if regime in {"TRANSITIONAL"}:
        return False

    if regime == "TREND_UP" and getattr(context, "market_stress_score", 0.0) < 0.35:
        return False

    if symbol is None:
        return regime in {"TREND_DOWN", "HIGH_STRESS", "RANGE"}

    features = resolve_symbol_features(context, symbol)
    if features is None:
        return regime in {"TREND_DOWN", "HIGH_STRESS", "RANGE"}

    micro_regime = getattr(features, "micro_regime", "UNDEFINED")
    if micro_regime == "UNDEFINED":
        return regime in {"TREND_DOWN", "HIGH_STRESS", "RANGE"}
    if micro_regime == "TREND_UP":
        return False
    return micro_regime in {"TREND_DOWN", "RANGE", "VOLATILE", "TRANSITIONAL"}


def supports_grid_trading(
    context: LiveMarketContext | None,
    symbol: str | None = None,
) -> bool:
    if context is None:
        return True

    regime = getattr(context, "market_regime", "UNDEFINED")
    if regime in {"UNDEFINED", None}:
        if getattr(context, "market_stress_score", 1.0) >= 0.35:
            return False
        if getattr(context, "advancers_ratio", 0.5) <= 0.45:
            return False
        return getattr(context, "long_tailwind", 0.0) > 0

    if regime not in {"RANGE", "TRANSITIONAL"}:
        return False
    if getattr(context, "market_stress_score", 1.0) >= 0.35:
        return False

    features = resolve_symbol_features(context, symbol) if symbol is not None else None
    if features is None:
        return True

    micro_regime = getattr(features, "micro_regime", "UNDEFINED")
    micro_transition = getattr(features, "micro_regime_transition", None)
    if micro_regime == "UNDEFINED":
        return True
    if micro_regime == "VOLATILE":
        return False
    if micro_transition in {"BREAKDOWN", "BREAKOUT_UP"}:
        return False
    return micro_regime in {"RANGE", "TRANSITIONAL"}

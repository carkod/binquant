from __future__ import annotations

from market_regime.models import (
    LiveMarketContext,
    MarketRegime,
    MarketRegimeTransition,
    MicroRegime,
    MicroRegimeTransition,
    SymbolMarketFeatures,
)
from shared.utils import clamp, non_negative


class RegimeTransitionDetector:
    """
    Shared macro and micro regime classifier.

    The detector labels the current market environment and compares it with
    the prior snapshot so strategies can react to transitions instead of only
    raw point-in-time scores.
    """

    _transition_strength_floor = 0.08

    def annotate_context(
        self,
        context: LiveMarketContext,
        previous_context: LiveMarketContext | None = None,
    ) -> LiveMarketContext:
        self._annotate_market_regime(
            context=context,
            previous_context=previous_context,
        )

        previous_symbol_features = (
            previous_context.symbol_features if previous_context is not None else {}
        )
        for symbol, features in context.symbol_features.items():
            self._annotate_symbol_regime(
                features=features,
                previous_features=previous_symbol_features.get(symbol),
            )
        return context

    def _annotate_market_regime(
        self,
        context: LiveMarketContext,
        previous_context: LiveMarketContext | None,
    ) -> None:
        breadth_score = clamp((context.advancers_ratio - 0.5) / 0.25)
        trend_participation = clamp(
            ((context.pct_above_ema20 + context.pct_above_ema50) - 1.0) * 1.4
        )
        avg_trend_bias = clamp(context.average_trend_score * 20.0)
        calm_score = clamp(1.0 - context.market_stress_score, 0.0, 1.0)

        long_score = clamp(
            0.3 * non_negative(context.long_tailwind)
            + 0.24 * non_negative(context.btc_regime_score)
            + 0.2 * non_negative(breadth_score)
            + 0.14 * non_negative(trend_participation)
            + 0.12 * calm_score,
            0.0,
            1.0,
        )
        short_score = clamp(
            0.28 * non_negative(context.short_tailwind)
            + 0.24 * non_negative(-context.btc_regime_score)
            + 0.16 * non_negative(-breadth_score)
            + 0.1 * non_negative(-avg_trend_bias)
            + 0.22 * context.market_stress_score,
            0.0,
            1.0,
        )
        range_score = clamp(
            0.32 * (1.0 - abs(breadth_score))
            + 0.22 * (1.0 - abs(context.btc_regime_score))
            + 0.24 * calm_score
            + 0.12 * (1.0 - abs(avg_trend_bias))
            + 0.1 * (1.0 - abs(context.long_tailwind - context.short_tailwind)),
            0.0,
            1.0,
        )
        stress_score = clamp(
            0.7 * context.market_stress_score
            + 0.18 * non_negative(-context.average_return * 20.0)
            + 0.12 * non_negative(short_score - long_score),
            0.0,
            1.0,
        )

        regime: MarketRegime = "TRANSITIONAL"
        dominant_score = max(long_score, short_score, range_score, stress_score)
        if stress_score >= 0.5 and context.market_stress_score >= 0.35:
            regime = "HIGH_STRESS"
        elif long_score >= 0.44 and long_score >= short_score + 0.08:
            regime = "TREND_UP"
        elif short_score >= 0.42 and short_score >= long_score + 0.08:
            regime = "TREND_DOWN"
        elif range_score >= 0.5:
            regime = "RANGE"

        previous_regime: MarketRegime | None = (
            previous_context.market_regime if previous_context else None
        )
        transition: MarketRegimeTransition | None = None
        transition_strength = 0.0
        regime_is_transitioning = regime == "TRANSITIONAL"

        if (
            previous_context is not None
            and previous_regime is not None
            and previous_regime != regime
        ):
            transition = self._market_transition_event(
                previous_regime=previous_regime,
                current_regime=regime,
            )
            previous_scores = (
                previous_context.long_regime_score,
                previous_context.short_regime_score,
                previous_context.range_regime_score,
                previous_context.stress_regime_score,
            )
            current_scores = (long_score, short_score, range_score, stress_score)
            transition_strength = clamp(
                dominant_score
                + max(
                    abs(curr - prev)
                    for curr, prev in zip(current_scores, previous_scores, strict=False)
                )
                - 0.25,
                0.0,
                1.0,
            )
            regime_is_transitioning = (
                regime_is_transitioning
                or transition_strength >= self._transition_strength_floor
            )

        context.market_regime = regime
        context.previous_market_regime = previous_regime
        context.market_regime_transition = transition
        context.market_regime_transition_strength = transition_strength
        context.long_regime_score = long_score
        context.short_regime_score = short_score
        context.range_regime_score = range_score
        context.stress_regime_score = stress_score
        context.regime_is_transitioning = regime_is_transitioning

    def _annotate_symbol_regime(
        self,
        features: SymbolMarketFeatures,
        previous_features: SymbolMarketFeatures | None,
    ) -> None:
        up_score = clamp(
            0.45 * non_negative(features.trend_score * 30.0)
            + 0.2 * float(features.above_ema20)
            + 0.15 * float(features.above_ema50)
            + 0.2 * non_negative(features.relative_strength_vs_btc * 20.0),
            0.0,
            1.0,
        )
        down_score = clamp(
            0.45 * non_negative(-features.trend_score * 30.0)
            + 0.2 * float(not features.above_ema20)
            + 0.15 * float(not features.above_ema50)
            + 0.2 * non_negative(-features.relative_strength_vs_btc * 20.0),
            0.0,
            1.0,
        )
        range_score = clamp(
            0.38 * (1.0 - min(abs(features.trend_score) * 30.0, 1.0))
            + 0.34 * (1.0 - min(features.bb_width / 0.08, 1.0))
            + 0.28 * (1.0 - min(features.atr_pct / 0.04, 1.0)),
            0.0,
            1.0,
        )
        volatile_score = clamp(
            0.55 * min(features.atr_pct / 0.05, 1.0)
            + 0.45 * min(features.bb_width / 0.12, 1.0),
            0.0,
            1.0,
        )

        regime: MicroRegime = "TRANSITIONAL"
        regime_strength = max(up_score, down_score, range_score, volatile_score)
        if volatile_score >= 0.72 and abs(features.return_pct) >= 0.015:
            regime = "VOLATILE"
        elif up_score >= 0.52 and up_score >= down_score + 0.1:
            regime = "TREND_UP"
        elif down_score >= 0.52 and down_score >= up_score + 0.1:
            regime = "TREND_DOWN"
        elif range_score >= 0.5:
            regime = "RANGE"

        previous_regime: MicroRegime | None = (
            previous_features.micro_regime if previous_features else None
        )
        transition: MicroRegimeTransition | None = None
        transition_strength = 0.0
        if (
            previous_features is not None
            and previous_regime is not None
            and previous_regime != regime
        ):
            transition = self._symbol_transition_event(
                previous_regime=previous_regime,
                current_regime=regime,
            )
            previous_strength = previous_features.micro_regime_strength
            transition_strength = clamp(
                regime_strength + abs(regime_strength - previous_strength) - 0.25,
                0.0,
                1.0,
            )

        features.micro_regime = regime
        features.micro_regime_strength = regime_strength
        features.micro_regime_transition = transition
        features.micro_regime_transition_strength = transition_strength

    @staticmethod
    def _market_transition_event(
        previous_regime: MarketRegime,
        current_regime: MarketRegime,
    ) -> MarketRegimeTransition:
        if current_regime == "HIGH_STRESS":
            return "STRESS_SPIKE"
        if previous_regime == "HIGH_STRESS" and current_regime != "HIGH_STRESS":
            return "STRESS_RELIEF"
        if current_regime == "TREND_UP":
            return "ENTERED_TREND_UP"
        if current_regime == "TREND_DOWN":
            return "ENTERED_TREND_DOWN"
        if current_regime == "RANGE":
            return "ENTERED_RANGE"
        return "LOST_REGIME_EDGE"

    @staticmethod
    def _symbol_transition_event(
        previous_regime: MicroRegime,
        current_regime: MicroRegime,
    ) -> MicroRegimeTransition:
        if current_regime == "VOLATILE":
            return "VOLATILITY_EXPANSION"
        if (
            previous_regime in {"RANGE", "TRANSITIONAL"}
            and current_regime == "TREND_UP"
        ):
            return "BREAKOUT_UP"
        if (
            previous_regime in {"RANGE", "TRANSITIONAL"}
            and current_regime == "TREND_DOWN"
        ):
            return "BREAKDOWN"
        if previous_regime == "TREND_DOWN" and current_regime == "TREND_UP":
            return "RECOVERY"
        if previous_regime == "TREND_UP" and current_regime == "RANGE":
            return "MEAN_REVERSION"
        if current_regime == "TREND_UP":
            return "ENTERED_TREND_UP"
        if current_regime == "TREND_DOWN":
            return "ENTERED_TREND_DOWN"
        if current_regime == "RANGE":
            return "ENTERED_RANGE"
        return "ENTERED_TRANSITIONAL"

from market_regime_prediction.models import LiveMarketContext, MarketContextScore


def _clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _non_negative(value: float) -> float:
    return max(0.0, float(value))


class BaseMarketContextModel:
    """
    Deterministic reusable whole-market conditioning model.

    Is the broader market supportive for a candidate's direction?
    How strongly the symbol can overcome weak breadth on its own?
    """

    def evaluate(
        self,
        symbol: str,
        direction: str,
        snapshot_or_live_context: LiveMarketContext | None,
        local_features: dict[str, float] | None = None,
    ) -> MarketContextScore:
        normalized_direction = direction.upper().strip()
        context = snapshot_or_live_context
        if context is None or context.confidence <= 0:
            return self._empty_score(symbol, normalized_direction)

        symbol_features = context.symbol_features.get(symbol)
        symbol_rs = self._resolve_feature(
            local_features=local_features,
            feature_name="relative_strength_vs_btc",
            fallback=(
                symbol_features.relative_strength_vs_btc if symbol_features else 0.0
            ),
        )
        symbol_trend = self._resolve_feature(
            local_features=local_features,
            feature_name="trend_score",
            fallback=(symbol_features.trend_score if symbol_features else 0.0),
        )

        if normalized_direction == "SHORT":
            breadth_score = context.short_tailwind
            btc_alignment_score = _clamp(-context.btc_regime_score)
            cross_asset_confirmation = _clamp(
                0.6 * (-symbol_rs) + 0.4 * (-symbol_trend)
            )
            override_strength = _clamp(
                0.6 * _non_negative(-symbol_rs) + 0.4 * _non_negative(-symbol_trend),
                0.0,
                1.0,
            )
            directional_stress = context.market_stress_score * 0.35
        else:
            breadth_score = context.long_tailwind
            btc_alignment_score = _clamp(context.btc_regime_score)
            cross_asset_confirmation = _clamp(0.6 * symbol_rs + 0.4 * symbol_trend)
            override_strength = _clamp(
                0.6 * _non_negative(symbol_rs) + 0.4 * _non_negative(symbol_trend),
                0.0,
                1.0,
            )
            directional_stress = -context.market_stress_score

        supportiveness_score = _clamp(
            0.35 * breadth_score
            + 0.25 * btc_alignment_score
            + 0.25 * cross_asset_confirmation
            + 0.15 * directional_stress
        )
        followthrough_score = _clamp(
            0.45 * breadth_score
            + 0.3 * btc_alignment_score
            + 0.25 * cross_asset_confirmation
        )
        adverse_excursion_risk = _clamp(
            0.55 * context.market_stress_score
            + 0.25 * _non_negative(-supportiveness_score)
            + 0.2 * (1.0 - override_strength),
            0.0,
            1.0,
        )

        if (
            normalized_direction == "LONG"
            and breadth_score < 0
            and override_strength > 0
        ):
            supportiveness_score = _clamp(
                supportiveness_score + 0.2 * override_strength
            )
            followthrough_score = _clamp(followthrough_score + 0.15 * override_strength)
        if (
            normalized_direction == "SHORT"
            and breadth_score < 0
            and override_strength > 0
        ):
            supportiveness_score = _clamp(
                supportiveness_score + 0.1 * override_strength
            )

        return MarketContextScore(
            symbol=symbol,
            direction=normalized_direction,
            context_timestamp=context.timestamp,
            confidence=context.confidence,
            long_tailwind=context.long_tailwind,
            short_tailwind=context.short_tailwind,
            breadth_score=breadth_score,
            btc_alignment_score=btc_alignment_score,
            cross_asset_confirmation=cross_asset_confirmation,
            market_stress_score=context.market_stress_score,
            followthrough_score=followthrough_score,
            adverse_excursion_risk=adverse_excursion_risk,
            override_strength=override_strength,
            supportiveness_score=supportiveness_score,
            metadata={
                "symbol_in_snapshot": symbol_features is not None,
                "symbol_relative_strength_vs_btc": symbol_rs,
                "symbol_trend_score": symbol_trend,
            },
        )

    @staticmethod
    def _resolve_feature(
        local_features: dict[str, float] | None,
        feature_name: str,
        fallback: float,
    ) -> float:
        if local_features and feature_name in local_features:
            return float(local_features[feature_name])
        return float(fallback)

    @staticmethod
    def _empty_score(symbol: str, direction: str) -> MarketContextScore:
        return MarketContextScore(
            symbol=symbol,
            direction=direction,
            context_timestamp=None,
            confidence=0.0,
            long_tailwind=0.0,
            short_tailwind=0.0,
            breadth_score=0.0,
            btc_alignment_score=0.0,
            cross_asset_confirmation=0.0,
            market_stress_score=0.0,
            followthrough_score=0.0,
            adverse_excursion_risk=0.0,
            override_strength=0.0,
            supportiveness_score=0.0,
        )

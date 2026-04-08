from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from market_regime_prediction.market_context_model import BaseMarketContextModel
from market_regime_prediction.models import LiveMarketContext, MarketContextScore


class SignalContextScorer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    context_weight: float = Field(default=1.0, ge=0.0)
    risk_weight: float = Field(default=0.5, ge=0.0)
    support_weight: float = Field(default=0.35, ge=0.0)

    def adjust_score(
        self,
        local_score: float,
        context_score: MarketContextScore,
    ) -> float:
        context_delta = (
            context_score.confidence
            * self.context_weight
            * (
                context_score.followthrough_score
                + (self.support_weight * context_score.supportiveness_score)
                - (self.risk_weight * context_score.adverse_excursion_risk)
            )
        )
        return float(local_score) + context_delta

    def evaluate_adjusted_score(
        self,
        symbol: str,
        direction: str,
        local_score: float,
        market_context: LiveMarketContext | None,
        context_model: BaseMarketContextModel,
        local_features: dict[str, float] | None = None,
    ) -> tuple[float, MarketContextScore]:
        context_score = context_model.evaluate(
            symbol=symbol,
            direction=direction,
            snapshot_or_live_context=market_context,
            local_features=local_features,
        )
        adjusted_score = self.adjust_score(
            local_score=local_score,
            context_score=context_score,
        )
        return adjusted_score, context_score

from __future__ import annotations

from typing import Any, TYPE_CHECKING, Protocol

from pydantic import BaseModel, ConfigDict, Field
from market_regime_prediction.market_context_model import BaseMarketContextModel
from market_regime_prediction.models import LiveMarketContext, MarketContextScore
from market_regime_prediction.signal_context_scorer import SignalContextScorer

if TYPE_CHECKING:
    pass


class SignalCandidateLike(Protocol):
    symbol: str
    direction: str
    score: float


class SignalContextEvaluation(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    candidate: Any
    adjusted_score: float
    emit: bool = Field(default=True)
    context_score: MarketContextScore


def score_signal_candidate_with_context(
    candidate: SignalCandidateLike,
    market_context: LiveMarketContext | None,
    context_model: BaseMarketContextModel,
    scorer: SignalContextScorer,
    local_features: dict[str, float] | None = None,
    emit_threshold: float | None = None,
) -> SignalContextEvaluation:
    """
    Shared integration seam for strategies.

    Local setup detection stays inside the strategy. Before emission, the
    candidate passes through this market-context layer for score adjustment.
    """

    adjusted_score, context_score = scorer.evaluate_adjusted_score(
        symbol=candidate.symbol,
        direction=candidate.direction,
        local_score=candidate.score,
        market_context=market_context,
        context_model=context_model,
        local_features=local_features,
    )
    candidate.score = adjusted_score
    emit = emit_threshold is None or adjusted_score >= emit_threshold
    return SignalContextEvaluation(
        candidate=candidate,
        adjusted_score=adjusted_score,
        emit=emit,
        context_score=context_score,
    )

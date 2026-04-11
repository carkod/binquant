from pybinbot import SignalsConsumer
from market_regime_prediction.models import (
    LiveMarketContext,
    SignalContextEvaluation,
)
from market_regime_prediction.signal_context_scorer import SignalContextScorer


def score_signal_candidate_with_context(
    candidate: SignalsConsumer,
    market_context: LiveMarketContext | None,
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

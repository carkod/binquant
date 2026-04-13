from market_regime_prediction.models import (
    LiveMarketContext,
    SignalContextEvaluation,
)
from market_regime_prediction.signal_context_scorer import SignalContextScorer


def score_signal_candidate_with_context(
    symbol: str,
    direction: str,
    score: float,
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
        symbol=symbol,
        direction=direction,
        local_score=score,
        market_context=market_context,
        local_features=local_features,
    )
    emit = emit_threshold is None or adjusted_score >= emit_threshold
    evaluation = SignalContextEvaluation(
        symbol=symbol,
        direction=direction,
        local_score=score,
        local_features=local_features,
        adjusted_score=adjusted_score,
        context_score=context_score,
        emit=emit,
    )
    return evaluation

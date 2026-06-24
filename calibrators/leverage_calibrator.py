import logging

from pybinbot import BinbotApi, ExchangeId, SymbolModel
from market_regime.models import LiveMarketContext


class LeverageCalibrator:
    """
    Dynamically sets per-symbol `futures_leverage` based on market regime.

    Decision ladder (capped at MAX_LEVERAGE = 3):
      1. Expensive coins (close >= price_high_threshold) stay at 1x.
      2. Defensive regimes (HIGH_STRESS, TRANSITIONAL) → 1x.
      3. High market stress (market_stress_score > stress_threshold) → 1x.
      4. Low confidence (< confidence_floor) → 1x.
      5. Spiky symbols (atr_pct > atr_high_threshold) → 1x.
      6. RANGE → 2x.
      7. TREND_UP / TREND_DOWN → 3x.
      8. Default → 1x.

    Writes back via `binbot_api.edit_symbol` only when target differs from
    the symbol's current `futures_leverage`. The binbot backend caps
    `futures_leverage` to [1, 3] in SymbolRequestPayload, so the cap is
    enforced server-side too.
    """

    MAX_LEVERAGE = 3
    DEFAULT_PRICE_HIGH_THRESHOLD = 500.0
    DEFAULT_STRESS_THRESHOLD = 0.7
    DEFAULT_CONFIDENCE_FLOOR = 0.5
    DEFAULT_ATR_HIGH_THRESHOLD = 0.04

    def __init__(
        self,
        binbot_api: BinbotApi,
        exchange: ExchangeId,
        *,
        price_high_threshold: float = DEFAULT_PRICE_HIGH_THRESHOLD,
        stress_threshold: float = DEFAULT_STRESS_THRESHOLD,
        confidence_floor: float = DEFAULT_CONFIDENCE_FLOOR,
        atr_high_threshold: float = DEFAULT_ATR_HIGH_THRESHOLD,
    ) -> None:
        self.binbot_api = binbot_api
        self.exchange = exchange
        self.price_high_threshold = price_high_threshold
        self.stress_threshold = stress_threshold
        self.confidence_floor = confidence_floor
        self.atr_high_threshold = atr_high_threshold

    def target_leverage(
        self,
        symbol: str,
        close: float,
        context: LiveMarketContext,
    ) -> int:
        if close >= self.price_high_threshold:
            return 1

        regime = context.market_regime
        if regime in ("HIGH_STRESS", "TRANSITIONAL"):
            return 1

        if context.market_stress_score > self.stress_threshold:
            return 1

        if context.confidence < self.confidence_floor:
            return 1

        features = context.get_symbol_features(symbol)
        if features is not None and features.atr_pct > self.atr_high_threshold:
            return 1

        if regime == "RANGE":
            return 2

        if regime in ("TREND_UP", "TREND_DOWN"):
            return self.MAX_LEVERAGE

        return 1

    def calibrate_all(
        self,
        context: LiveMarketContext,
        all_symbols: list[SymbolModel],
    ) -> dict[str, int]:
        """
        Iterate over symbols with live features, write new leverage when it
        differs from the current row. Returns a small counters dict for
        observability.
        """
        rows_by_id = {row.id: row for row in all_symbols}
        applied = 0
        no_change = 0
        skipped = 0

        for symbol, features in context.symbol_features.items():
            row = rows_by_id.get(symbol)
            if row is None:
                skipped += 1
                continue

            current = row.futures_leverage
            target = self.target_leverage(symbol, features.close, context)

            if target == current:
                no_change += 1
                continue

            try:
                self.binbot_api.edit_symbol(
                    symbol=symbol, exchange_id=self.exchange, futures_leverage=target
                )
                row.futures_leverage = target
                applied += 1
            except Exception:
                logging.exception(
                    "[LeverageCalibrator] failed to update %s -> %s", symbol, target
                )
                skipped += 1

        logging.info(
            "[LeverageCalibrator] applied=%d no_change=%d skipped=%d",
            applied,
            no_change,
            skipped,
        )
        return {"applied": applied, "no_change": no_change, "skipped": skipped}

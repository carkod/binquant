from collections import deque
from collections.abc import Mapping
from typing import Any

from pandas import DataFrame, Series, concat

from market_regime_prediction.market_state_store import MarketStateStore
from market_regime_prediction.models import LiveMarketContext, SymbolMarketFeatures
from shared.utils import clamp, safe_pct

REQUIRED_FRESH_SYMBOLS = 40


class LiveMarketContextAccumulator:
    """
    Rebuilds the same timestamp snapshot incrementally as fresh closed candles arrive.
    This ensures that context becomes available as soon as possible
    and gets refined with every new candle for the same timestamp.

    A context becomes available only when BTC is fresh and at least 40 symbols
    are fresh for that exact timestamp.
    """

    def __init__(
        self,
        state_store: MarketStateStore,
        btc_symbol: str,
    ) -> None:
        self.state_store = state_store
        self.btc_symbol = btc_symbol
        self._contexts_by_timestamp: dict[int, LiveMarketContext] = {}
        self._context_order: deque[int] = deque(maxlen=64)

    def on_closed_candle(
        self,
        symbol: str,
        candle: Mapping[str, Any] | Series | DataFrame,
    ) -> LiveMarketContext | None:
        history = self.state_store.update(symbol=symbol, candle=candle)
        timestamp = int(history.iloc[-1]["timestamp"])
        context = self._build_context(timestamp)
        if context is None:
            return None

        self._contexts_by_timestamp[timestamp] = context
        if timestamp not in self._context_order:
            self._context_order.append(timestamp)
        return context

    def get_context(self, timestamp: int) -> LiveMarketContext | None:
        return self._contexts_by_timestamp.get(timestamp)

    def get_latest_context(self) -> LiveMarketContext | None:
        while self._context_order:
            timestamp = self._context_order[-1]
            context = self._contexts_by_timestamp.get(timestamp)
            if context is not None:
                return context
            self._context_order.pop()
        return None

    def refresh_context_for_timestamp(self, timestamp: int) -> LiveMarketContext | None:
        context = self._build_context(timestamp)
        if context is None:
            return None
        self._contexts_by_timestamp[timestamp] = context
        if timestamp not in self._context_order:
            self._context_order.append(timestamp)
        return context

    def _build_context(self, timestamp: int) -> LiveMarketContext | None:
        fresh_symbols = self.state_store.get_fresh_symbols(timestamp)
        if (
            self.btc_symbol not in fresh_symbols
            or len(fresh_symbols) < REQUIRED_FRESH_SYMBOLS
        ):
            return None

        symbol_features: dict[str, SymbolMarketFeatures] = {}
        btc_features: SymbolMarketFeatures | None = None

        for symbol in fresh_symbols:
            history = self.state_store.get_symbol_history(symbol)
            features = self._compute_symbol_features(symbol, history)
            if features is None:
                continue
            symbol_features[symbol] = features
            if symbol == self.btc_symbol:
                btc_features = features

        if btc_features is None:
            return None

        for symbol, features in symbol_features.items():
            if symbol == self.btc_symbol:
                continue
            features.relative_strength_vs_btc = (
                features.return_pct - btc_features.return_pct
            )

        effective_count = len(symbol_features)
        if effective_count < REQUIRED_FRESH_SYMBOLS:
            return None

        advancers = sum(1 for item in symbol_features.values() if item.return_pct > 0)
        decliners = sum(1 for item in symbol_features.values() if item.return_pct < 0)
        advancers_ratio = advancers / effective_count
        decliners_ratio = decliners / effective_count
        advancers_decliners_ratio = advancers / max(decliners, 1)
        average_return = (
            sum(item.return_pct for item in symbol_features.values()) / effective_count
        )
        average_rs = (
            sum(item.relative_strength_vs_btc for item in symbol_features.values())
            / effective_count
        )
        pct_above_ema20 = (
            sum(1 for item in symbol_features.values() if item.above_ema20)
            / effective_count
        )
        pct_above_ema50 = (
            sum(1 for item in symbol_features.values() if item.above_ema50)
            / effective_count
        )
        average_trend_score = (
            sum(item.trend_score for item in symbol_features.values()) / effective_count
        )
        average_atr_pct = (
            sum(item.atr_pct for item in symbol_features.values()) / effective_count
        )
        average_bb_width = (
            sum(item.bb_width for item in symbol_features.values()) / effective_count
        )

        breadth_balance = clamp((advancers_ratio - decliners_ratio) * 1.5)
        ema_balance = clamp(((pct_above_ema20 + pct_above_ema50) - 1.0) * 1.5)
        average_return_score = clamp(average_return * 12.0)
        btc_regime_score = clamp(
            btc_features.return_pct * 12.0 + btc_features.trend_score * 6.0
        )
        stress_from_volatility = clamp((average_atr_pct - 0.02) * 12.0, 0.0, 1.0)
        stress_from_bandwidth = clamp((average_bb_width - 0.08) * 4.0, 0.0, 1.0)
        stress_from_selloff = clamp((-average_return) * 16.0, 0.0, 1.0)
        market_stress_score = (
            0.4 * stress_from_volatility
            + 0.25 * stress_from_bandwidth
            + 0.35 * stress_from_selloff
        )
        long_tailwind = clamp(
            0.4 * breadth_balance
            + 0.2 * ema_balance
            + 0.25 * btc_regime_score
            + 0.15 * average_return_score
            - 0.35 * market_stress_score
        )
        short_tailwind = clamp(
            -0.35 * breadth_balance
            - 0.15 * ema_balance
            - 0.2 * btc_regime_score
            - 0.15 * average_return_score
            + 0.45 * market_stress_score
        )

        total_tracked_symbols = max(
            len(self.state_store.get_tracked_symbols()),
            effective_count,
        )
        coverage_ratio = (
            effective_count / total_tracked_symbols if total_tracked_symbols else 0.0
        )

        return LiveMarketContext(
            timestamp=timestamp,
            fresh_count=effective_count,
            total_tracked_symbols=total_tracked_symbols,
            coverage_ratio=coverage_ratio,
            btc_symbol=self.btc_symbol,
            btc_present=True,
            confidence=1.0,
            is_provisional=False,
            advancers=advancers,
            decliners=decliners,
            advancers_ratio=advancers_ratio,
            decliners_ratio=decliners_ratio,
            advancers_decliners_ratio=advancers_decliners_ratio,
            average_return=average_return,
            average_relative_strength_vs_btc=average_rs,
            pct_above_ema20=pct_above_ema20,
            pct_above_ema50=pct_above_ema50,
            average_trend_score=average_trend_score,
            average_atr_pct=average_atr_pct,
            average_bb_width=average_bb_width,
            btc_return=btc_features.return_pct,
            btc_trend_score=btc_features.trend_score,
            btc_regime_score=btc_regime_score,
            market_stress_score=market_stress_score,
            long_tailwind=long_tailwind,
            short_tailwind=short_tailwind,
            symbol_features=symbol_features,
            metadata={
                "fresh_symbols": sorted(symbol_features.keys()),
                "fresh_symbol_count": effective_count,
            },
        )

    @staticmethod
    def _compute_symbol_features(
        symbol: str,
        history: DataFrame,
    ) -> SymbolMarketFeatures | None:
        if history.empty or len(history) < 2:
            return None

        working = history.copy().sort_values("timestamp").reset_index(drop=True)
        closes = working["close"].astype(float)
        highs = working["high"].astype(float)
        lows = working["low"].astype(float)
        previous_close = closes.shift(1)
        true_range = concat(
            [
                highs - lows,
                (highs - previous_close).abs(),
                (lows - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)

        ema20 = closes.ewm(span=20, adjust=False, min_periods=1).mean().iloc[-1]
        ema50 = closes.ewm(span=50, adjust=False, min_periods=1).mean().iloc[-1]
        atr = true_range.rolling(14, min_periods=1).mean().iloc[-1]
        mid = closes.rolling(20, min_periods=1).mean()
        std = closes.rolling(20, min_periods=1).std(ddof=0).fillna(0.0)
        bb_upper = mid + (2 * std)
        bb_lower = mid - (2 * std)

        latest_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2])
        atr_pct = float(atr / latest_close) if latest_close else 0.0
        bb_width = (
            float((bb_upper.iloc[-1] - bb_lower.iloc[-1]) / abs(mid.iloc[-1]))
            if mid.iloc[-1]
            else 0.0
        )
        trend_score = float((ema20 - ema50) / abs(ema50)) if float(ema50) != 0 else 0.0

        return SymbolMarketFeatures(
            symbol=symbol,
            timestamp=int(working.iloc[-1]["timestamp"]),
            close=latest_close,
            return_pct=safe_pct(latest_close, prev_close),
            ema20=float(ema20),
            ema50=float(ema50),
            above_ema20=latest_close > float(ema20),
            above_ema50=latest_close > float(ema50),
            trend_score=trend_score,
            relative_strength_vs_btc=0.0,
            atr_pct=atr_pct,
            bb_width=bb_width,
        )

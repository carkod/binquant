import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pybinbot import ExchangeId, GridDeploymentRequest, MarketType, SignalsConsumer
from market_regime.regime_routing import resolve_symbol_features

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LadderDeployer:
    ALGO = "grid_ladder"
    ENABLED = True
    AUTOTRADE = True
    MIN_RANGE_WIDTH_PCT = 1.5
    MAX_RANGE_WIDTH_PCT = 8.0
    MIN_BREAKOUT_BUFFER_PCT = 0.5
    MAX_BREAKOUT_BUFFER_PCT = 4.0
    BREAKOUT_ATR_MULTIPLIER = 1.5
    # Don't deploy into a market where breadth is overwhelmingly bearish.
    MIN_LONG_REGIME_SCORE = 0.2
    MIN_BB_WIDTH_STABILITY_CANDLES = 8
    MAX_BB_WIDTH_CHANGE_PCT = 20.0
    ALLOWED_MARKET_REGIMES = ("RANGE",)
    ALLOWED_MICRO_REGIMES = ("RANGE", "TRANSITIONAL")
    BLOCKING_MICRO_TRANSITIONS = (
        "BREAKDOWN",
        "VOLATILITY_EXPANSION",
        "ENTERED_TREND_DOWN",
    )

    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer

    def _bb_stable(self, n: int, max_change_pct: float) -> bool:
        df = self.ti.df_15m.tail(n)
        if len(df) < n:
            return False
        widths = []
        for _, row in df.iterrows():
            mid = float(row.bb_mid)
            if mid <= 0:
                return False
            width = (float(row.bb_upper) - float(row.bb_lower)) / mid
            if width <= 0:
                return False
            widths.append(width)
        change_pct = abs((widths[-1] - widths[0]) / widths[0]) * 100
        return change_pct <= max_change_pct

    async def signal(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        if not self.ENABLED:
            return
        # binbot's grid-ladder endpoint only accepts FUTURES; emitting a
        # SPOT grid signal would always 400 at the API. Skip early.
        if self.ti.market_type != MarketType.FUTURES:
            logging.info("grid_ladder skipped: market_type_not_futures")
            return
        context = self.ti.latest_market_context
        if context is None or context.market_regime not in self.ALLOWED_MARKET_REGIMES:
            logging.info("grid_ladder skipped: market_regime")
            return
        if context.regime_is_transitioning:
            logging.info("grid_ladder skipped: market_transitioning")
            return
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        if (
            symbol_features is None
            or symbol_features.micro_regime not in self.ALLOWED_MICRO_REGIMES
        ):
            logging.info("grid_ladder skipped: symbol_micro_regime")
            return
        if symbol_features.micro_regime_transition in self.BLOCKING_MICRO_TRANSITIONS:
            logging.info("grid_ladder skipped: symbol_transition")
            return
        if context.long_regime_score < self.MIN_LONG_REGIME_SCORE:
            logging.info("grid_ladder skipped: long_regime_score_too_low")
            return
        if not self._bb_stable(
            self.MIN_BB_WIDTH_STABILITY_CANDLES,
            self.MAX_BB_WIDTH_CHANGE_PCT,
        ):
            logging.info("grid_ladder skipped: bb_width_expanding")
            return
        range_low = float(bb_low)
        range_high = float(bb_high)
        if not (range_low < current_price < range_high):
            logging.info("grid_ladder skipped: price_outside_range")
            return
        range_width_pct = (
            ((range_high - range_low) / float(bb_mid)) * 100 if bb_mid > 0 else 0
        )
        if not (
            self.MIN_RANGE_WIDTH_PCT <= range_width_pct <= self.MAX_RANGE_WIDTH_PCT
        ):
            logging.info("grid_ladder skipped: range_width")
            return
        raw_buffer = symbol_features.atr_pct * 100 * self.BREAKOUT_ATR_MULTIPLIER
        breakout_buffer_pct = max(
            self.MIN_BREAKOUT_BUFFER_PCT,
            min(self.MAX_BREAKOUT_BUFFER_PCT, raw_buffer),
        )
        context_payload = context.model_dump(mode="json") if context else {}
        settings = self.at_consumer.autotrade_settings
        exchange = ExchangeId(self.ti.exchange)
        market_type = MarketType(self.ti.market_type)
        grid_params = GridDeploymentRequest(
            symbol=self.symbol,
            fiat=settings.fiat,
            exchange=exchange,
            market_type=market_type,
            algorithm_name=self.ALGO,
            generated_at=datetime.now(UTC),
            range_low=range_low,
            range_high=range_high,
            breakout_low=range_low * (1 - breakout_buffer_pct / 100),
            breakout_high=range_high * (1 + breakout_buffer_pct / 100),
            total_margin=settings.grid_total_margin,
            level_count=settings.grid_level_count,
            current_price=current_price,
            current_regime=context.market_regime,
            context=context_payload,
            indicators={
                "bb_high": bb_high,
                "bb_mid": bb_mid,
                "bb_low": bb_low,
                "range_width_pct": range_width_pct,
                "atr_buffer_pct": breakout_buffer_pct,
            },
            allocation_pct=settings.grid_allocation_pct,
            cash_reserve_pct=settings.grid_cash_reserve_pct,
        )
        grid_params.exchange = exchange
        grid_params.market_type = market_type
        value = SignalsConsumer(
            signal_kind="grid_deploy",
            direction="grid",
            current_price=current_price,
            autotrade=self.AUTOTRADE,
            grid_params=grid_params,
        )
        # Run autotrade gate first so it can resolve the actual total_margin
        # on the signal (the value built above carries a candidate placeholder).
        # Then dispatch the analytics record so the row reflects what was
        # actually deployed.
        await self.at_consumer.process_autotrade_restrictions(value)
        self.ti.dispatch_signal_record(value=value)

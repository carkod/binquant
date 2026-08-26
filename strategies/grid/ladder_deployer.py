import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pybinbot import ExchangeId, GridDeploymentRequest, MarketType, SignalsConsumer
from market_regime.regime_routing import resolve_symbol_features

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LadderDeployer:
    ALGO = "grid_ladder"
    AUTOTRADE = True
    MIN_RANGE_WIDTH_PCT = 1.5
    MAX_RANGE_WIDTH_PCT = 8.0
    MIN_BREAKOUT_BUFFER_PCT = 0.5
    MAX_BREAKOUT_BUFFER_PCT = 4.0
    BREAKOUT_ATR_MULTIPLIER = 1.5
    MIN_LONG_REGIME_SCORE = 0.2
    MIN_ENTRY_CONTRACTS = 2
    DISABLE_UPPER_BAND_SHORT_ENTRIES = True
    FIRST_CYCLE_TIMEOUT_HOURS = 12
    MAX_LIFETIME_HOURS = 12
    MAX_COMPLETED_CYCLES = 2
    MIN_BB_WIDTH_STABILITY_CANDLES = 8
    MAX_BB_WIDTH_CHANGE_PCT = 20.0
    ALLOWED_MICRO_REGIMES = ("RANGE",)
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
        settings = self.at_consumer.autotrade_settings
        if not settings.enable_grid_ladders:
            logging.info("grid_ladder skipped: enable_grid_ladders_disabled")
            return
        # binbot's grid-ladder endpoint only accepts FUTURES; emitting a
        # SPOT grid signal would always 400 at the API. Skip early.
        if self.ti.market_type != MarketType.FUTURES:
            logging.info("grid_ladder skipped: market_type_not_futures")
            return
        context = self.ti.latest_market_context
        grid_only_policy = self.ti.grid_only_policy
        if not grid_only_policy.allow_grid_ladder:
            logging.info(
                "grid_ladder skipped: grid_only_policy_%s",
                grid_only_policy.reason,
            )
            return
        if context is None:
            logging.info("grid_ladder skipped: market_context_unavailable")
            return
        if context.market_regime != "RANGE":
            logging.info("grid_ladder skipped: market_regime_not_range")
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
        if not symbol_features.above_ema20 and not symbol_features.above_ema50:
            logging.info("grid_ladder skipped: symbol_below_ema20_and_ema50")
            return
        if symbol_features.relative_strength_vs_btc <= 0:
            logging.info("grid_ladder skipped: relative_strength_vs_btc_not_positive")
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
        existing_grid_context = context_payload.get("grid_ladder", {})
        if not isinstance(existing_grid_context, dict):
            existing_grid_context = {}
        context_payload["grid_ladder"] = {
            **existing_grid_context,
            "disable_upper_band_short_entries": self.DISABLE_UPPER_BAND_SHORT_ENTRIES,
            "first_cycle_timeout_hours": self.FIRST_CYCLE_TIMEOUT_HOURS,
            "max_lifetime_hours": self.MAX_LIFETIME_HOURS,
            "max_completed_cycles": self.MAX_COMPLETED_CYCLES,
            "max_bb_width_change_pct": self.MAX_BB_WIDTH_CHANGE_PCT,
            "min_entry_contracts": self.MIN_ENTRY_CONTRACTS,
        }
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
                "disable_upper_band_short_entries": self.DISABLE_UPPER_BAND_SHORT_ENTRIES,
                "first_cycle_timeout_hours": self.FIRST_CYCLE_TIMEOUT_HOURS,
                "max_lifetime_hours": self.MAX_LIFETIME_HOURS,
                "max_completed_cycles": self.MAX_COMPLETED_CYCLES,
                "max_bb_width_change_pct": self.MAX_BB_WIDTH_CHANGE_PCT,
                "min_entry_contracts": self.MIN_ENTRY_CONTRACTS,
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
        # Persist first so the ladder create payload can link back to this signal.
        await self.ti.dispatch_signal_record(value=value)
        await self.at_consumer.process_autotrade_restrictions(value)

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pybinbot import GridDeploymentRequest, MarketType, SignalsConsumer
from market_regime.regime_routing import resolve_symbol_features

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LadderDeployer:
    ALGO = "grid_ladder"
    ENABLED = True
    AUTOTRADE = True
    GRID_ALLOCATION_PCT = 1.0
    CASH_RESERVE_PCT = 0.0
    MIN_RANGE_WIDTH_PCT = 1.5
    MAX_RANGE_WIDTH_PCT = 8.0
    BREAKOUT_BUFFER_PCT = 0.6
    MAX_ACTIVE_LADDERS = 3  # move to autotrade settings
    MIN_BB_WIDTH_STABILITY_CANDLES = 8
    MAX_BB_WIDTH_CHANGE_PCT = 20.0
    ALLOWED_MARKET_REGIMES = ("RANGE",)
    ALLOWED_MICRO_REGIMES = ("RANGE", "TRANSITIONAL")
    BLOCKING_MICRO_TRANSITIONS = (
        "BREAKDOWN",
        "VOLATILITY_EXPANSION",
        "ENTERED_TREND_DOWN",
    )
    # autotrade_consumer overwrites this with the real margin before POSTing.
    # Kept >0 so GridDeploymentRequest's validator (total_margin > 0) accepts it.
    PLACEHOLDER_TOTAL_MARGIN = 1.0

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
        breakout_buffer_pct = self.BREAKOUT_BUFFER_PCT
        context_payload = context.model_dump(mode="json") if context else {}
        grid_params = GridDeploymentRequest(
            symbol=self.symbol,
            fiat=str(self.at_consumer.autotrade_settings["fiat"]),
            exchange=self.ti.exchange,
            market_type=self.ti.market_type,
            algorithm_name=self.ALGO,
            generated_at=datetime.now(UTC),
            range_low=range_low,
            range_high=range_high,
            breakout_low=range_low * (1 - breakout_buffer_pct / 100),
            breakout_high=range_high * (1 + breakout_buffer_pct / 100),
            total_margin=self.PLACEHOLDER_TOTAL_MARGIN,
            level_count=3,
            current_price=current_price,
            current_regime=context.market_regime,
            context=context_payload,
            indicators={
                "bb_high": bb_high,
                "bb_mid": bb_mid,
                "bb_low": bb_low,
                "range_width_pct": range_width_pct,
            },
            allocation_pct=self.GRID_ALLOCATION_PCT,
            cash_reserve_pct=self.CASH_RESERVE_PCT,
        )
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

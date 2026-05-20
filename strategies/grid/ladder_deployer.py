import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pybinbot import SignalsConsumer
from market_regime.regime_routing import resolve_symbol_features

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LadderDeployer:
    ALGO = "grid_ladder"
    DEFAULT_CONFIG_PATH = (
        Path(__file__).resolve().parents[2] / "config" / "grid_ladder.default.json"
    )
    OVERRIDE_CONFIG_PATH = (
        Path(__file__).resolve().parents[2] / ".runtime" / "grid_ladder.json"
    )

    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.last_deploy: dict[str, datetime] = {}

    def _cfg(self) -> dict[str, Any]:
        base = {}
        for path in (self.DEFAULT_CONFIG_PATH, self.OVERRIDE_CONFIG_PATH):
            if path.exists():
                try:
                    base.update(json.loads(path.read_text()).get("grid_ladder", {}))
                except Exception:
                    logging.exception("Failed reading %s", path)
        return base

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
        cfg = self._cfg()
        if not cfg.get("enabled", False):
            return
        context = self.ti.latest_market_context
        if context is None or context.market_regime not in cfg.get(
            "allowed_market_regimes", ["RANGE"]
        ):
            logging.info("grid_ladder skipped: market_regime")
            return
        if context.regime_is_transitioning:
            logging.info("grid_ladder skipped: market_transitioning")
            return
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        if symbol_features is None or symbol_features.micro_regime not in cfg.get(
            "allowed_micro_regimes", ["RANGE"]
        ):
            logging.info("grid_ladder skipped: symbol_micro_regime")
            return
        if symbol_features.micro_regime_transition in set(
            cfg.get("blocking_micro_transitions", [])
        ):
            logging.info("grid_ladder skipped: symbol_transition")
            return
        if not self._bb_stable(
            int(cfg.get("min_bb_width_stability_candles", 8)),
            float(cfg.get("max_bb_width_change_pct", 20.0)),
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
            float(cfg.get("min_range_width_pct", 1.5))
            <= range_width_pct
            <= float(cfg.get("max_range_width_pct", 8.0))
        ):
            logging.info("grid_ladder skipped: range_width")
            return
        if range_width_pct <= 2.5:
            levels = 5
        elif range_width_pct <= 5.0:
            levels = 7
        else:
            levels = 9
        breakout_buffer_pct = float(cfg.get("breakout_buffer_pct", 0.6))
        grid_params = {
            "symbol": self.symbol,
            "range_low": range_low,
            "range_high": range_high,
            "breakout_low": range_low * (1 - breakout_buffer_pct / 100),
            "breakout_high": range_high * (1 + breakout_buffer_pct / 100),
            "level_count": levels,
            "range_width_pct": range_width_pct,
            "algorithm": self.ALGO,
        }
        value = SignalsConsumer(
            signal_kind="grid_deploy",
            direction="grid",
            current_price=current_price,
            autotrade=cfg.get("autotrade", False),
            grid_params=grid_params,
        )
        self.ti.dispatch_signal_record(value=value)
        await self.at_consumer.process_autotrade_restrictions(value)

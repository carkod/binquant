import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    Position,
    SignalsConsumer,
    round_numbers,
)
from market_regime.models import LiveMarketContext
from market_regime.regime_routing import is_regime_stable, resolve_symbol_features
from models.strategies import GridSignalDecision
from shared.strategy_mixin import StrategyMixin
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class GridTrading(StrategyMixin):
    """
    Coinrule-style grid logic.

    Signals are intentionally allowed to forward-test outside the strict
    RANGE/RANGE gate so we can study how often the Coinrule-style 2% move
    appears in production. Autotrade routing remains stricter and is reported
    in the alert payload.
    """

    ALGO = "coinrule_grid_trading"
    CLIP_SIZE_QUOTE = 20.0
    LEVERAGE = 3
    DEFAULT_BUY_TRIGGER_PCT = 0.02
    DEFAULT_SELL_TRIGGER_PCT = 0.02
    DEFAULT_ANCHOR_WINDOW_CANDLES = 8
    ANCHOR_WINDOW_CANDLES = DEFAULT_ANCHOR_WINDOW_CANDLES
    LOOKBACK_CANDLES = DEFAULT_ANCHOR_WINDOW_CANDLES + 1
    AUTOTRADE_STRESS_THRESHOLD = 0.35
    REPO_ROOT = Path(__file__).resolve().parents[2]
    DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "grid_thresholds.default.json"
    DEFAULT_OVERRIDE_CONFIG_PATH = REPO_ROOT / ".runtime" / "grid_thresholds.json"
    SHORT_AUTOTRADE_MICRO_REGIMES = {"RANGE", "TRANSITIONAL", "TREND_DOWN"}
    _config_cache_key: tuple[str, int | None, int | None] | None = None
    _config_cache: dict[str, Any] | None = None

    def __init__(
        self,
        cls: "ContextEvaluator",
        buy_trigger_pct: float | None = None,
        sell_trigger_pct: float | None = None,
        anchor_window_candles: int | None = None,
    ) -> None:
        self.ti = cls
        self.df_15m = cls.df_15m
        self.config = cls.config
        self.binbot_api = cls.binbot_api
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context
        self.runtime_config_path = self._resolve_override_config_path()
        self.buy_trigger_pct = self.DEFAULT_BUY_TRIGGER_PCT
        self.sell_trigger_pct = self.DEFAULT_SELL_TRIGGER_PCT
        self.ANCHOR_WINDOW_CANDLES = self.DEFAULT_ANCHOR_WINDOW_CANDLES
        self.LOOKBACK_CANDLES = self.DEFAULT_ANCHOR_WINDOW_CANDLES + 1
        self.refresh_runtime_config(
            buy_trigger_pct=buy_trigger_pct,
            sell_trigger_pct=sell_trigger_pct,
            anchor_window_candles=anchor_window_candles,
        )

    @property
    def latest_market_context(self):
        return self.ti.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value) -> None:
        self.ti.latest_market_context = value

    @classmethod
    def _resolve_override_config_path(cls) -> Path:
        configured = os.getenv("GRID_TRADING_CONFIG_PATH")
        if configured:
            return Path(configured).expanduser()
        return cls.DEFAULT_OVERRIDE_CONFIG_PATH

    @classmethod
    def _read_json_file(cls, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            logging.exception("Failed to read grid config file: %s", path)
            return {}
        if not isinstance(data, dict):
            logging.warning("Grid config file must contain a JSON object: %s", path)
            return {}
        return data

    @classmethod
    def _get_strategy_config(cls) -> dict[str, Any]:
        override_path = cls._resolve_override_config_path()
        default_mtime = (
            cls.DEFAULT_CONFIG_PATH.stat().st_mtime_ns
            if cls.DEFAULT_CONFIG_PATH.exists()
            else None
        )
        override_mtime = (
            override_path.stat().st_mtime_ns if override_path.exists() else None
        )
        cache_key = (str(override_path), default_mtime, override_mtime)
        if cls._config_cache_key == cache_key and cls._config_cache is not None:
            return dict(cls._config_cache)

        default_config = cls._read_json_file(cls.DEFAULT_CONFIG_PATH)
        override_config = cls._read_json_file(override_path)
        merged = dict(default_config.get(cls.ALGO, {}))
        merged.update(override_config.get(cls.ALGO, {}))
        cls._config_cache_key = cache_key
        cls._config_cache = dict(merged)
        return merged

    def refresh_runtime_config(
        self,
        buy_trigger_pct: float | None = None,
        sell_trigger_pct: float | None = None,
        anchor_window_candles: int | None = None,
    ) -> None:
        runtime_config = self._get_strategy_config()
        resolved_buy = (
            runtime_config.get("buy_trigger_pct")
            if buy_trigger_pct is None
            else buy_trigger_pct
        )
        resolved_sell = (
            runtime_config.get("sell_trigger_pct")
            if sell_trigger_pct is None
            else sell_trigger_pct
        )
        resolved_anchor_window = (
            runtime_config.get("anchor_window_candles")
            if anchor_window_candles is None
            else anchor_window_candles
        )

        self.buy_trigger_pct = self._coerce_positive_float(
            resolved_buy,
            fallback=self.DEFAULT_BUY_TRIGGER_PCT,
            label="buy_trigger_pct",
        )
        self.sell_trigger_pct = self._coerce_positive_float(
            resolved_sell,
            fallback=self.DEFAULT_SELL_TRIGGER_PCT,
            label="sell_trigger_pct",
        )
        self.ANCHOR_WINDOW_CANDLES = self._coerce_anchor_window(resolved_anchor_window)
        self.LOOKBACK_CANDLES = self.ANCHOR_WINDOW_CANDLES + 1

    @staticmethod
    def _coerce_positive_float(
        value: Any,
        *,
        fallback: float,
        label: str,
    ) -> float:
        if value is None:
            return fallback
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            logging.warning(
                "Invalid grid config for %s=%r, using %s", label, value, fallback
            )
            return fallback
        if parsed <= 0:
            logging.warning("Grid config for %s must be > 0, using %s", label, fallback)
            return fallback
        return parsed

    def _coerce_anchor_window(self, value: Any) -> int:
        if value is None:
            return self.DEFAULT_ANCHOR_WINDOW_CANDLES
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            logging.warning(
                "Invalid grid config for anchor_window_candles=%r, using %s",
                value,
                self.DEFAULT_ANCHOR_WINDOW_CANDLES,
            )
            return self.DEFAULT_ANCHOR_WINDOW_CANDLES
        if parsed < 2:
            logging.warning(
                "Grid config for anchor_window_candles must be >= 2, using %s",
                self.DEFAULT_ANCHOR_WINDOW_CANDLES,
            )
            return self.DEFAULT_ANCHOR_WINDOW_CANDLES
        return parsed

    @classmethod
    def supports_grid_trading(
        cls,
        context: LiveMarketContext | None,
    ) -> tuple[bool, str]:
        """
        Keep grid autotrade gating focused on broad market range quality. Grid
        entries should stay available across symbol-level micro regimes when
        the broader market is calm and range-bound. A fresh RANGE label is
        allowed through before the stability timer matures; explicit
        transitions, stress, and non-range regimes remain hard blockers.
        """
        if context is None:
            return False, "market_context_unavailable"
        if context.regime_is_transitioning:
            return False, "market_transitioning"
        if context.market_stress_score >= cls.AUTOTRADE_STRESS_THRESHOLD:
            return False, "market_stress_too_high"
        if context.market_regime != "RANGE":
            return False, f"market_regime_{str(context.market_regime).lower()}"
        if not is_regime_stable(context):
            return True, "market_range_unstable_allowed"
        return True, "market_range_stable"

    @staticmethod
    def _resolve_directional_autotrade(
        *,
        action: str,
        base_autotrade_eligible: bool,
        base_autotrade_route: str,
        symbol_features: Any,
    ) -> tuple[bool, str]:
        if not base_autotrade_eligible:
            return False, base_autotrade_route
        if action == "sell" and (
            symbol_features is None
            or symbol_features.micro_regime
            not in GridTrading.SHORT_AUTOTRADE_MICRO_REGIMES
        ):
            return False, "symbol_regime_not_shortable_for_grid_short"
        return True, base_autotrade_route

    def _anchor_metrics(
        self,
        recent_window,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> dict[str, float]:
        anchor_window = recent_window["close"].iloc[:-1].astype(float)
        anchor_price = float(anchor_window.median()) if not anchor_window.empty else 0.0
        anchor_high = float(anchor_window.max()) if not anchor_window.empty else 0.0
        anchor_low = float(anchor_window.min()) if not anchor_window.empty else 0.0
        range_width = (
            (anchor_high - anchor_low) / anchor_price if anchor_price > 0 else 0.0
        )
        first_anchor = float(anchor_window.iloc[0]) if not anchor_window.empty else 0.0
        last_anchor = float(anchor_window.iloc[-1]) if not anchor_window.empty else 0.0
        range_drift = (
            (last_anchor - first_anchor) / first_anchor if first_anchor > 0 else 0.0
        )
        band_span = float(bb_high - bb_low)
        band_position = (
            (float(current_price) - float(bb_low)) / band_span if band_span > 0 else 0.5
        )
        bb_width = float(band_span / bb_mid) if float(bb_mid) > 0 else 0.0
        trend_slope_proxy = (
            (float(current_price) - anchor_price) / anchor_price
            if anchor_price > 0
            else 0.0
        )
        return {
            "anchor_price": anchor_price,
            "anchor_high": anchor_high,
            "anchor_low": anchor_low,
            "range_width": range_width,
            "range_drift": range_drift,
            "bb_width": bb_width,
            "band_position": band_position,
            "trend_slope_proxy": trend_slope_proxy,
        }

    def evaluate(
        self,
        recent_window,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> GridSignalDecision:
        anchor_metrics = self._anchor_metrics(
            recent_window=recent_window,
            current_price=float(current_price),
            bb_high=float(bb_high),
            bb_mid=float(bb_mid),
            bb_low=float(bb_low),
        )
        live_anchor = anchor_metrics["anchor_price"]
        rsi_value = (
            float(recent_window["rsi"].iloc[-1])
            if "rsi" in recent_window.columns
            else 50.0
        )

        if live_anchor <= 0:
            return GridSignalDecision(
                should_trigger=False,
                reason="Invalid live anchor for percentage move calculation.",
                anchor_price=live_anchor,
                anchor_high=anchor_metrics["anchor_high"],
                anchor_low=anchor_metrics["anchor_low"],
                anchor_window_candles=self.ANCHOR_WINDOW_CANDLES,
                range_width=anchor_metrics["range_width"],
                range_drift=anchor_metrics["range_drift"],
                bb_width=anchor_metrics["bb_width"],
                band_position=anchor_metrics["band_position"],
                trend_slope_proxy=anchor_metrics["trend_slope_proxy"],
            )

        move_from_anchor = (
            (current_price - live_anchor) / live_anchor if live_anchor > 0 else 0.0
        )

        buy_zone = move_from_anchor <= -self.buy_trigger_pct
        sell_zone = move_from_anchor >= self.sell_trigger_pct

        if buy_zone:
            return GridSignalDecision(
                should_trigger=True,
                action="buy",
                reason=(
                    "Price is down enough from the broader live anchor to trigger "
                    f"the grid BUY leg ({move_from_anchor * 100:.2f}%)."
                ),
                anchor_price=live_anchor,
                anchor_high=anchor_metrics["anchor_high"],
                anchor_low=anchor_metrics["anchor_low"],
                anchor_window_candles=self.ANCHOR_WINDOW_CANDLES,
                trigger_move_pct=self.buy_trigger_pct,
                range_width=anchor_metrics["range_width"],
                range_drift=anchor_metrics["range_drift"],
                bb_width=anchor_metrics["bb_width"],
                band_position=anchor_metrics["band_position"],
                rsi_value=rsi_value,
                trend_slope_proxy=anchor_metrics["trend_slope_proxy"],
            )

        if sell_zone:
            return GridSignalDecision(
                should_trigger=True,
                action="sell",
                reason=(
                    "Price is up enough from the broader live anchor to trigger "
                    f"the grid SELL leg ({move_from_anchor * 100:.2f}%)."
                ),
                anchor_price=live_anchor,
                anchor_high=anchor_metrics["anchor_high"],
                anchor_low=anchor_metrics["anchor_low"],
                anchor_window_candles=self.ANCHOR_WINDOW_CANDLES,
                trigger_move_pct=self.sell_trigger_pct,
                range_width=anchor_metrics["range_width"],
                range_drift=anchor_metrics["range_drift"],
                bb_width=anchor_metrics["bb_width"],
                band_position=anchor_metrics["band_position"],
                rsi_value=rsi_value,
                trend_slope_proxy=anchor_metrics["trend_slope_proxy"],
            )

        return GridSignalDecision(
            should_trigger=False,
            reason=(
                "Price has not moved far enough from the broader live anchor for "
                "the manual +/-2% grid rule."
            ),
            anchor_price=live_anchor,
            anchor_high=anchor_metrics["anchor_high"],
            anchor_low=anchor_metrics["anchor_low"],
            anchor_window_candles=self.ANCHOR_WINDOW_CANDLES,
            trigger_move_pct=self.buy_trigger_pct,
            range_width=anchor_metrics["range_width"],
            range_drift=anchor_metrics["range_drift"],
            bb_width=anchor_metrics["bb_width"],
            band_position=anchor_metrics["band_position"],
            rsi_value=rsi_value,
            trend_slope_proxy=anchor_metrics["trend_slope_proxy"],
        )

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        """
        Coinrule grid signal based on a +/-2% move from a broader rolling anchor.
        """
        self.refresh_runtime_config()
        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context, self.symbol)
        autotrade_eligible, autotrade_route = self.supports_grid_trading(
            context=context,
        )

        self.df_15m = self.ti.df_15m.copy()
        if len(self.df_15m) < self.LOOKBACK_CANDLES:
            logging.warning(
                "15m candles grid trading not enough data for symbol: %s",
                self.symbol,
            )
            return

        required_cols = ["close"]
        recent_window = self.df_15m.tail(self.LOOKBACK_CANDLES)
        if recent_window[required_cols].isnull().any().any():
            logging.info("Grid skipped: recent data contains nulls.")
            return

        decision = self.evaluate(
            recent_window=recent_window,
            current_price=float(current_price),
            bb_high=float(bb_high),
            bb_mid=float(bb_mid),
            bb_low=float(bb_low),
        )

        if not decision.should_trigger or decision.action is None:
            logging.info("Grid skipped: %s", decision.reason)
            return

        autotrade, autotrade_route = self._resolve_directional_autotrade(
            action=decision.action,
            base_autotrade_eligible=autotrade_eligible,
            base_autotrade_route=autotrade_route,
            symbol_features=symbol_features,
        )

        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        live_anchor = decision.anchor_price
        move_from_anchor = (
            (float(current_price) - live_anchor) / live_anchor
            if live_anchor > 0
            else 0.0
        )

        if decision.action == "sell":
            action_label = "SHORT ENTRY"
            bot_strategy = Position.short
            grid_logic = (
                f"Simple +{self.sell_trigger_pct * 100:.1f}% grid short "
                f"trigger from the {decision.anchor_window_candles}-candle anchor"
            )
            action_text = (
                f"SELL ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} as market order "
                f"with {self.LEVERAGE}x leverage after a +{self.sell_trigger_pct * 100:.1f}% move "
                "from the live anchor"
            )
        else:
            action_label = "LONG ENTRY"
            bot_strategy = Position.long
            grid_logic = (
                f"Simple -{self.buy_trigger_pct * 100:.1f}% grid long "
                f"trigger from the {decision.anchor_window_candles}-candle anchor"
            )
            action_text = (
                f"BUY ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} as market order "
                f"using isolated margin with {self.LEVERAGE}x leverage after a "
                f"-{self.buy_trigger_pct * 100:.1f}% move from the live anchor"
            )

        msg = f"""
            - [{os.getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {action_label}
            - Current price: {round_numbers(current_price, 6)}
            - Live anchor price: {round_numbers(live_anchor, 6)}
            - Anchor window: {decision.anchor_window_candles} candles
            - Anchor range: {round_numbers(decision.anchor_low, 6)} - {round_numbers(decision.anchor_high, 6)}
            - Move from live anchor: {round_numbers(move_from_anchor * 100, 2)}%
            - Strategy: {bot_strategy.value}
            - Rule intent: {action_text}
            - Market regime: {context.market_regime if context is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Grid logic: {grid_logic}
            - Order setup: market order, isolated margin, {self.LEVERAGE}x leverage
            - Range width: {round_numbers(decision.range_width * 100, 2)}%
            - Range drift: {round_numbers(decision.range_drift * 100, 2)}%
            - BB width: {round_numbers(decision.bb_width * 100, 2)}%
            - Band position: {round_numbers(decision.band_position, 3)}
            - RSI: {round_numbers(decision.rsi_value, 2)}
            - Reason: {decision.reason}
            - Autotrade candidate: {"Yes" if autotrade else "No"}
            - Autotrade route: {autotrade_route}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=bot_strategy,
                market_type=self.market_type,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.ti.dispatch_signal_record(
            value=value,
            indicators={
                "grid_anchor_price": live_anchor,
                "grid_anchor_high": decision.anchor_high,
                "grid_anchor_low": decision.anchor_low,
                "grid_anchor_window_candles": decision.anchor_window_candles,
                "move_from_anchor_pct": move_from_anchor,
                "grid_range_width": decision.range_width,
                "grid_range_drift": decision.range_drift,
                "grid_bb_width": decision.bb_width,
                "grid_band_position": decision.band_position,
                "grid_autotrade_candidate": autotrade,
                "grid_autotrade_route": autotrade_route,
            },
        )
        self.telegram_consumer.dispatch_signal(msg)
        if autotrade:
            await self.at_consumer.process_autotrade_restrictions(value)

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.models import LiveMarketContext
from market_regime.regime_routing import is_regime_stable, resolve_symbol_features
from models.strategies import BBExtremeReversionDecision
from shared.strategy_mixin import StrategyMixin
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class BBExtremeReversion(StrategyMixin):
    """
    Connors-style RSI(2) mean reversion at Bollinger Band extremes.

    Replaces the original coinrule_grid_trading strategy. The old rolling-anchor
    +/-2% trigger fired during continuations as often as during reversions
    (90% loss rate historically). This version only fires on statistical
    exhaustion:

      BUY:  RSI(2) <= 5   AND price <= bb_lower  AND market_regime == RANGE
      SELL: RSI(2) >= 95  AND price >= bb_upper  AND market_regime == RANGE

    Far fewer signals than the grid trigger but each one is a real statistical
    extreme. Exit is delegated to
    binbot/api/exchange_apis/kucoin/futures/position_market.py, which provides
    ATR-based emergency SL plus BB-derived dynamic trailing (see
    BB_EXTREME_REVERSION_ALGO dispatch there — the constant is this file's ALGO).
    """

    ALGO = "bb_extreme_reversion"
    CLIP_SIZE_QUOTE = 20.0
    LEVERAGE = 3

    DEFAULT_RSI_WINDOW = 2
    DEFAULT_OVERSOLD_RSI = 5.0
    DEFAULT_OVERBOUGHT_RSI = 95.0
    # "At or beyond the band" — 0.0 = exactly at bb_lower, < 0 = below it.
    DEFAULT_MAX_LOWER_BAND_POSITION = 0.0
    DEFAULT_MIN_UPPER_BAND_POSITION = 1.0

    AUTOTRADE_STRESS_THRESHOLD = 0.35
    AUTOTRADE_MARKET_REGIMES = {"RANGE"}
    SHORT_AUTOTRADE_MICRO_REGIMES = {"RANGE", "TRANSITIONAL", "TREND_DOWN"}
    MICRO_REGIME_BLOCKING_TRANSITIONS = {
        "VOLATILITY_EXPANSION",
        "BREAKDOWN",
        "ENTERED_TRANSITIONAL",
    }
    MICRO_REGIME_MIN_STRENGTH = 0.5

    REPO_ROOT = Path(__file__).resolve().parents[2]
    DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "bb_extreme_reversion.default.json"
    DEFAULT_OVERRIDE_CONFIG_PATH = REPO_ROOT / ".runtime" / "bb_extreme_reversion.json"
    LOOKBACK_CANDLES = 30  # plenty for RSI(2); BB spreads arrive from the caller

    _config_cache_key: tuple[str, int | None, int | None] | None = None
    _config_cache: dict[str, Any] | None = None

    def __init__(
        self,
        cls: "ContextEvaluator",
        rsi_window: int | None = None,
        oversold_rsi: float | None = None,
        overbought_rsi: float | None = None,
        max_lower_band_position: float | None = None,
        min_upper_band_position: float | None = None,
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
        self.rsi_window = self.DEFAULT_RSI_WINDOW
        self.oversold_rsi = self.DEFAULT_OVERSOLD_RSI
        self.overbought_rsi = self.DEFAULT_OVERBOUGHT_RSI
        self.max_lower_band_position = self.DEFAULT_MAX_LOWER_BAND_POSITION
        self.min_upper_band_position = self.DEFAULT_MIN_UPPER_BAND_POSITION
        self.refresh_runtime_config(
            rsi_window=rsi_window,
            oversold_rsi=oversold_rsi,
            overbought_rsi=overbought_rsi,
            max_lower_band_position=max_lower_band_position,
            min_upper_band_position=min_upper_band_position,
        )

    @property
    def latest_market_context(self):
        return self.ti.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value) -> None:
        self.ti.latest_market_context = value

    @classmethod
    def _resolve_override_config_path(cls) -> Path:
        configured = os.getenv("BB_EXTREME_REVERSION_CONFIG_PATH")
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
            logging.exception(
                "Failed to read bb_extreme_reversion config file: %s", path
            )
            return {}
        if not isinstance(data, dict):
            logging.warning(
                "bb_extreme_reversion config file must contain a JSON object: %s", path
            )
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
        rsi_window: int | None = None,
        oversold_rsi: float | None = None,
        overbought_rsi: float | None = None,
        max_lower_band_position: float | None = None,
        min_upper_band_position: float | None = None,
    ) -> None:
        runtime_config = self._get_strategy_config()
        self.rsi_window = self._coerce_positive_int(
            rsi_window if rsi_window is not None else runtime_config.get("rsi_window"),
            fallback=self.DEFAULT_RSI_WINDOW,
            label="rsi_window",
        )
        self.oversold_rsi = self._coerce_rsi(
            oversold_rsi
            if oversold_rsi is not None
            else runtime_config.get("oversold_rsi"),
            fallback=self.DEFAULT_OVERSOLD_RSI,
            label="oversold_rsi",
        )
        self.overbought_rsi = self._coerce_rsi(
            overbought_rsi
            if overbought_rsi is not None
            else runtime_config.get("overbought_rsi"),
            fallback=self.DEFAULT_OVERBOUGHT_RSI,
            label="overbought_rsi",
        )
        self.max_lower_band_position = self._coerce_float(
            max_lower_band_position
            if max_lower_band_position is not None
            else runtime_config.get("max_lower_band_position"),
            fallback=self.DEFAULT_MAX_LOWER_BAND_POSITION,
            label="max_lower_band_position",
        )
        self.min_upper_band_position = self._coerce_float(
            min_upper_band_position
            if min_upper_band_position is not None
            else runtime_config.get("min_upper_band_position"),
            fallback=self.DEFAULT_MIN_UPPER_BAND_POSITION,
            label="min_upper_band_position",
        )

    @staticmethod
    def _coerce_positive_int(value: Any, *, fallback: int, label: str) -> int:
        if value is None:
            return fallback
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            logging.warning(
                "Invalid bb_extreme_reversion config for %s=%r, using %s",
                label,
                value,
                fallback,
            )
            return fallback
        if parsed < 2:
            logging.warning(
                "bb_extreme_reversion config for %s must be >= 2, using %s",
                label,
                fallback,
            )
            return fallback
        return parsed

    @staticmethod
    def _coerce_rsi(value: Any, *, fallback: float, label: str) -> float:
        if value is None:
            return fallback
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            logging.warning(
                "Invalid bb_extreme_reversion config for %s=%r, using %s",
                label,
                value,
                fallback,
            )
            return fallback
        if not 0.0 <= parsed <= 100.0:
            logging.warning(
                "bb_extreme_reversion config for %s must be in [0, 100], using %s",
                label,
                fallback,
            )
            return fallback
        return parsed

    @staticmethod
    def _coerce_float(value: Any, *, fallback: float, label: str) -> float:
        if value is None:
            return fallback
        try:
            return float(value)
        except (TypeError, ValueError):
            logging.warning(
                "Invalid bb_extreme_reversion config for %s=%r, using %s",
                label,
                value,
                fallback,
            )
            return fallback

    @classmethod
    def supports_autotrade(
        cls,
        context: LiveMarketContext | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"
        if context.regime_is_transitioning:
            return False, "market_transitioning"
        if context.market_stress_score >= cls.AUTOTRADE_STRESS_THRESHOLD:
            return False, "market_stress_too_high"
        if context.market_regime not in cls.AUTOTRADE_MARKET_REGIMES:
            return False, f"market_regime_{str(context.market_regime).lower()}"
        regime_slug = str(context.market_regime).lower()
        if not is_regime_stable(context):
            return True, f"market_{regime_slug}_unstable_allowed"
        return True, f"market_{regime_slug}_stable"

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
        if symbol_features is None:
            return False, "symbol_features_unavailable"
        transition = symbol_features.micro_regime_transition
        if transition in BBExtremeReversion.MICRO_REGIME_BLOCKING_TRANSITIONS:
            return False, f"symbol_transition_{str(transition).lower()}"
        if (
            symbol_features.micro_regime_strength
            < BBExtremeReversion.MICRO_REGIME_MIN_STRENGTH
        ):
            return False, "symbol_micro_regime_unstable"
        if (
            action == "sell"
            and symbol_features.micro_regime
            not in BBExtremeReversion.SHORT_AUTOTRADE_MICRO_REGIMES
        ):
            return False, "symbol_regime_not_shortable"
        if action == "buy" and symbol_features.micro_regime == "TREND_DOWN":
            return False, "symbol_regime_trend_down_for_long"
        return True, base_autotrade_route

    @staticmethod
    def _compute_rsi(closes: pd.Series, window: int) -> float | None:
        """RSI on the given close series. Returns None if there isn't
        enough data."""
        if len(closes) < window + 1:
            return None
        delta = closes.astype(float).diff()
        gain = delta.where(delta > 0, 0.0).rolling(window).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(window).mean()
        rs = gain / loss
        rsi_series = 100 - (100 / (1 + rs))
        val = rsi_series.iloc[-1]
        if pd.isna(val):
            return None
        # rs is +inf when loss==0 → rsi=100; rs is 0 when gain==0 → rsi=0.
        # pandas handles the inf path; explicit clamps for safety.
        return max(0.0, min(100.0, float(val)))

    def evaluate(
        self,
        recent_window,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> BBExtremeReversionDecision:
        rsi_value = self._compute_rsi(recent_window["close"], self.rsi_window)
        band_span = float(bb_high) - float(bb_low)
        band_position = (
            (float(current_price) - float(bb_low)) / band_span if band_span > 0 else 0.5
        )
        bb_width = band_span / float(bb_mid) if float(bb_mid) > 0 else 0.0
        distance_from_mid_pct = (
            (float(current_price) - float(bb_mid)) / float(bb_mid) * 100
            if float(bb_mid) > 0
            else 0.0
        )

        decision_defaults = {
            "rsi_window": self.rsi_window,
            "rsi_value": rsi_value if rsi_value is not None else 50.0,
            "band_position": band_position,
            "bb_width": bb_width,
            "bb_mid": float(bb_mid),
            "distance_from_mid_pct": distance_from_mid_pct,
        }

        if rsi_value is None:
            return BBExtremeReversionDecision(
                should_trigger=False,
                reason="Not enough candles to compute RSI.",
                **decision_defaults,
            )
        if band_span <= 0:
            return BBExtremeReversionDecision(
                should_trigger=False,
                reason="Invalid Bollinger band spread (band_span <= 0).",
                **decision_defaults,
            )

        if (
            rsi_value <= self.oversold_rsi
            and band_position <= self.max_lower_band_position
        ):
            return BBExtremeReversionDecision(
                should_trigger=True,
                action="buy",
                reason=(
                    f"RSI({self.rsi_window})={rsi_value:.1f} <= {self.oversold_rsi:.1f} "
                    f"and band_position={band_position:.2f} <= "
                    f"{self.max_lower_band_position:.2f}: oversold extreme."
                ),
                **decision_defaults,
            )

        if (
            rsi_value >= self.overbought_rsi
            and band_position >= self.min_upper_band_position
        ):
            return BBExtremeReversionDecision(
                should_trigger=True,
                action="sell",
                reason=(
                    f"RSI({self.rsi_window})={rsi_value:.1f} >= "
                    f"{self.overbought_rsi:.1f} and band_position="
                    f"{band_position:.2f} >= {self.min_upper_band_position:.2f}: "
                    "overbought extreme."
                ),
                **decision_defaults,
            )

        return BBExtremeReversionDecision(
            should_trigger=False,
            reason=(
                f"No extreme: RSI({self.rsi_window})={rsi_value:.1f} "
                f"band_position={band_position:.2f}."
            ),
            **decision_defaults,
        )

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        """
        Connors-style BB+RSI extreme mean-reversion signal. Replaces the old
        coinrule_grid_trading 2% trigger.
        """
        self.refresh_runtime_config()
        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context, self.symbol)
        autotrade_eligible, autotrade_route = self.supports_autotrade(context=context)

        self.df_15m = self.ti.df_15m.copy()
        if len(self.df_15m) < self.LOOKBACK_CANDLES:
            logging.warning(
                "15m candles bb_extreme_reversion not enough data for symbol: %s",
                self.symbol,
            )
            return

        required_cols = ["close"]
        recent_window = self.df_15m.tail(self.LOOKBACK_CANDLES)
        if recent_window[required_cols].isnull().any().any():
            logging.info("bb_extreme_reversion skipped: recent data contains nulls.")
            return

        decision = self.evaluate(
            recent_window=recent_window,
            current_price=float(current_price),
            bb_high=float(bb_high),
            bb_mid=float(bb_mid),
            bb_low=float(bb_low),
        )

        if not decision.should_trigger or decision.action is None:
            logging.info("bb_extreme_reversion skipped: %s", decision.reason)
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

        if decision.action == "sell":
            action_label = "SHORT ENTRY"
            bot_strategy = Position.short
            action_text = (
                f"SELL ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} as market order "
                f"with {self.LEVERAGE}x leverage at the overbought extreme; target "
                "reversion to bb_mid"
            )
        else:
            action_label = "LONG ENTRY"
            bot_strategy = Position.long
            action_text = (
                f"BUY ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} as market order "
                f"using isolated margin with {self.LEVERAGE}x leverage at the "
                "oversold extreme; target reversion to bb_mid"
            )

        msg = f"""
            - [{os.getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {action_label}
            - Current price: {round_numbers(current_price, 6)}
            - BB mid (reversion target): {round_numbers(bb_mid, 6)}
            - BB range: {round_numbers(bb_low, 6)} - {round_numbers(bb_high, 6)}
            - Distance from mid: {round_numbers(decision.distance_from_mid_pct, 2)}%
            - Strategy: {bot_strategy.value}
            - Rule intent: {action_text}
            - Market regime: {context.market_regime if context is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Order setup: market order, isolated margin, {self.LEVERAGE}x leverage
            - BB width: {round_numbers(decision.bb_width * 100, 2)}%
            - Band position: {round_numbers(decision.band_position, 3)}
            - RSI({decision.rsi_window}): {round_numbers(decision.rsi_value, 2)}
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
                "bb_extreme_rsi_window": decision.rsi_window,
                "bb_extreme_rsi_value": decision.rsi_value,
                "bb_extreme_band_position": decision.band_position,
                "bb_extreme_bb_width": decision.bb_width,
                "bb_extreme_bb_mid": decision.bb_mid,
                "bb_extreme_distance_from_mid_pct": decision.distance_from_mid_pct,
                "bb_extreme_autotrade_candidate": autotrade,
                "bb_extreme_autotrade_route": autotrade_route,
            },
        )
        self.telegram_consumer.dispatch_signal(msg)
        if autotrade:
            await self.at_consumer.process_autotrade_restrictions(value)

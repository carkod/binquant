import logging
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from pybinbot import (
    BotBase,
    HABollinguerSpread,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.strategy_mixin import StrategyMixin
from shared.time_of_day_filter import (
    build_quiet_hours_signal_msg,
    is_autotrade_suppressed,
)
from shared.utils import (
    build_links_msg,
    format_context_timestamp_line,
    normalize_timestamp,
    safe_pct,
)

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class BuyTheDip(StrategyMixin):
    ALGO = "coinrule_buy_the_dip"
    START_TIME = datetime(2026, 4, 12, 23, 21, tzinfo=UTC)
    LOOKBACK_HOURS = 6
    LOOKBACK_CANDLES = 24

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.df_15m = cls.df_15m
        self.config = cls.config
        self.binbot_api = cls.binbot_api
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context

    @property
    def latest_market_context(self) -> LiveMarketContext | None:
        return self.ti.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value: LiveMarketContext | None) -> None:
        self.ti.latest_market_context = value

    def _find_reference_price(self, target_time: datetime) -> float | None:
        if "close_time" not in self.df_15m.columns:
            return None

        for _, candle in self.df_15m.iloc[::-1].iterrows():
            candle_time = normalize_timestamp(candle.get("close_time"))
            if candle_time <= target_time:
                return float(candle["close"])
        return None

    def _reclaimed_prior_close_and_ema20(self, current_price: float) -> bool:
        prior_close, ema20 = self._prior_close_and_ema20()
        return current_price > prior_close and current_price > ema20

    def _prior_close_and_ema20(self) -> tuple[float, float]:
        prior_close = float(self.df_15m["close"].iloc[-2])
        ema20 = float(
            self.df_15m["close"]
            .ewm(span=20, adjust=False, min_periods=1)
            .mean()
            .iloc[-1]
        )
        return prior_close, ema20

    @staticmethod
    def _allows_entry(
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> bool:
        if context is not None and context.market_regime in {"TREND_DOWN", "TREND_UP"}:
            return False
        if symbol_features is not None and symbol_features.micro_regime in {
            "TREND_DOWN",
            "TREND_UP",
        }:
            return False
        return True

    @staticmethod
    def _allows_buy_the_dip_autotrade(
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> bool:
        if context is None:
            return False
        if context.regime_is_transitioning:
            return False
        if context.market_stress_score >= 0.35:
            return False
        if context.market_regime not in {"RANGE", "TRANSITIONAL"}:
            return False
        if symbol_features is None:
            return True
        if symbol_features.micro_regime in {"TREND_DOWN", "TREND_UP", "VOLATILE"}:
            return False
        return symbol_features.micro_regime in {"RANGE", "TRANSITIONAL"}

    @staticmethod
    def _resolve_autotrade_route(
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> str:
        if context is None:
            return "market_context_unavailable"
        if context.regime_is_transitioning:
            return "market_transitioning"
        if context.market_stress_score >= 0.35:
            return "market_stress_too_high"
        if context.market_regime not in {"RANGE", "TRANSITIONAL"}:
            return f"market_regime_{str(context.market_regime).lower()}"
        if symbol_features is None:
            return "symbol_regime_unavailable"
        if symbol_features.micro_regime in {"TREND_DOWN", "TREND_UP", "VOLATILE"}:
            return f"symbol_regime_{str(symbol_features.micro_regime).lower()}"
        if symbol_features.micro_regime in {"RANGE", "TRANSITIONAL"}:
            return f"symbol_regime_{str(symbol_features.micro_regime).lower()}"
        return "symbol_regime_unavailable"

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        self.df_15m = self.ti.df_15m.copy()
        required_cols = ["close", "close_time"]

        if (
            len(self.df_15m) < self.LOOKBACK_CANDLES
            or self.df_15m[required_cols].isnull().any().any()
        ):
            logging.info(
                "Buy-the-dip skipped: not enough 15m data for symbol %s",
                self.symbol,
            )
            return

        now = normalize_timestamp(self.df_15m["close_time"].iloc[-1])
        if now < self.START_TIME:
            return

        current_price = float(current_price)
        reference_price = self._find_reference_price(
            target_time=now - timedelta(hours=self.LOOKBACK_HOURS)
        )
        if reference_price is None:
            return

        change_6h = safe_pct(current_price, reference_price) * 100.0
        if change_6h > -2.0 or change_6h <= -5.0:
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )
        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        if not self._allows_entry(context=context, symbol_features=symbol_features):
            logging.info(
                "Buy-the-dip skipped: %s is in a blocked trend regime",
                self.symbol,
            )
            return
        if not self._reclaimed_prior_close_and_ema20(current_price):
            logging.info(
                "Buy-the-dip skipped: %s has not reclaimed prior close and EMA20",
                self.symbol,
            )
            return

        autotrade = self._allows_buy_the_dip_autotrade(
            context=context,
            symbol_features=symbol_features,
        )
        autotrade_route = self._resolve_autotrade_route(
            context=context,
            symbol_features=symbol_features,
        )

        if autotrade and is_autotrade_suppressed(context=context):
            autotrade = False
            autotrade_route = "time_of_day_quiet_hours"
            self.telegram_consumer.dispatch_signal(
                build_quiet_hours_signal_msg(
                    symbol=self.symbol,
                    algo=self.ALGO,
                    side=Position.long.value,
                    context=context,
                )
            )

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
        - Action: LONG ENTRY
        - Current price: {round_numbers(current_price, 6)}
        - Strategy: {Position.long.value}
        - Rule intent: BUY after a 6h dip between -2.0% and -5.0% once price reclaims the prior close and EMA20
        - Market regime: {context.market_regime if context is not None and context.market_regime is not None else "UNAVAILABLE"}
        - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
        {format_context_timestamp_line(context)}
        - Coin regime: {symbol_features.micro_regime if symbol_features is not None and symbol_features.micro_regime is not None else "UNAVAILABLE"}
        - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
        - 6h reference price: {round_numbers(reference_price, 6)}
        - 6h price change: {round_numbers(change_6h, 2)}%
        - Candle time: {now.isoformat()}
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
                position=Position.long,
                market_type=self.market_type,
                margin_short_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

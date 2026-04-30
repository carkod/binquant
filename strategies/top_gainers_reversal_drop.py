from os import getenv
from typing import TYPE_CHECKING

from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    KlineSchema,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopGainersReversalDrop:
    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.exchange = cls.exchange
        self.binbot_api = cls.binbot_api
        self.telegram_consumer = cls.telegram_consumer
        self.market_type = cls.market_type
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context
        self._breadth_cross_tolerance = cls._breadth_cross_tolerance
        self._autotrade_stress_threshold = cls._autotrade_stress_threshold
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.qty_precision = cls.qty_precision
        self.lookback_window = 20
        self.score_lookback = 80
        self.score_quantile = 0.9
        self.pump_lookback = 6
        self.cooldown_bars = 4
        self.min_baseline_volume = 1e-8
        self.min_pump_return = 0.04
        self.min_retrace_from_peak = 0.015
        self.min_volume_ratio = 1.8
        self.min_upper_wick_frac = 0.3
        self.max_close_position = 0.35

    @property
    def latest_market_context(self):
        return self.ti.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value) -> None:
        self.ti.latest_market_context = value

    def _allows_strategy_short_autotrade(self) -> bool:
        context = self.latest_market_context
        if context is None:
            return False
        if context.regime_is_transitioning:
            return False
        if context.market_regime is None:
            return False
        if context.market_regime == "TRANSITIONAL":
            return False
        if context.market_regime == "TREND_UP" and context.market_stress_score < 0.35:
            return False

        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        if symbol_features is None:
            return context.market_regime in {"TREND_DOWN", "HIGH_STRESS", "RANGE"}
        if symbol_features.micro_regime == "TREND_UP":
            return False
        return symbol_features.micro_regime in {
            "TREND_DOWN",
            "RANGE",
            "VOLATILE",
            "TRANSITIONAL",
        }

    def regime_routing(self) -> tuple[bool, str]:
        context = self.latest_market_context
        if context is None:
            return False, "market_context_unavailable"

        if context.market_regime is None:
            return False, "market_regime_unavailable"

        if context.market_regime in {"RANGE", "TREND_UP"}:
            return False, f"market_regime_{context.market_regime.lower()}"

        if self._allows_strategy_short_autotrade():
            return True, "short_autotrade_allowed"

        if context.regime_is_transitioning:
            return False, "market_transitioning"
        return False, f"market_regime_{context.market_regime.lower()}"

    def compute_indicators(
        self, df: TypedDataFrame[KlineSchema]
    ) -> TypedDataFrame[KlineSchema]:
        df = df.copy()
        baseline_window = max(self.lookback_window, 2)

        df["baseline_volume"] = (
            df["volume"]
            .shift(2)
            .rolling(window=baseline_window - 1, min_periods=baseline_window - 1)
            .median()
        )
        df["baseline_volume_safe"] = df["baseline_volume"].clip(
            lower=self.min_baseline_volume
        )
        df["volume_ratio"] = df["volume"] / df["baseline_volume_safe"]

        candle_range = (df["high"] - df["low"]).clip(lower=self.min_baseline_volume)
        upper_wick = df["high"] - df[["open", "close"]].max(axis=1)
        rolling_peak = df["high"].rolling(self.pump_lookback, min_periods=2).max()

        df["pump_return"] = (
            df["close"] / df["close"].shift(self.pump_lookback).clip(lower=1e-12)
        ) - 1
        df["retrace_from_peak"] = (rolling_peak - df["close"]) / rolling_peak.clip(
            lower=1e-12
        )
        df["upper_wick_frac"] = upper_wick.clip(lower=0) / candle_range
        df["close_position"] = (df["close"] - df["low"]) / candle_range
        df["bearish_reversal"] = (df["close"] < df["open"]) & (
            df["close"] < df["close"].shift(1)
        )
        df["pump_flag"] = df["pump_return"] > self.min_pump_return
        df["volume_confirmation"] = df["volume_ratio"] > self.min_volume_ratio
        df["exhaustion_flag"] = (df["upper_wick_frac"] > self.min_upper_wick_frac) & (
            df["close_position"] < self.max_close_position
        )
        df["retrace_flag"] = df["retrace_from_peak"] > self.min_retrace_from_peak
        df["reversal_drop_score"] = (
            df["pump_return"].clip(lower=0)
            * df["volume_ratio"]
            * (1 + df["upper_wick_frac"] + df["retrace_from_peak"].clip(lower=0))
        )
        df["score_threshold"] = (
            df["reversal_drop_score"]
            .shift(1)
            .rolling(self.score_lookback, min_periods=self.lookback_window)
            .quantile(self.score_quantile)
        )

        raw_signal = (
            df["bearish_reversal"]
            & df["pump_flag"]
            & df["volume_confirmation"]
            & df["exhaustion_flag"]
            & df["retrace_flag"]
            & (df["reversal_drop_score"] >= df["score_threshold"].fillna(0))
        )
        recent_signal = (
            raw_signal.shift(1)
            .rolling(self.cooldown_bars, min_periods=1)
            .max()
            .fillna(False)
            .astype(bool)
        )
        df["qualified_signal"] = raw_signal & ~recent_signal

        return df

    async def signal(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        df = self.ti.df_5m
        if (
            df is None
            or df.empty
            or len(df) < (self.lookback_window + self.pump_lookback)
        ):
            return None

        df = self.compute_indicators(df)
        row = df.iloc[-1]

        if not bool(row["qualified_signal"]):
            return None

        top_gainers = await self.binbot_api.get_top_gainers()
        gain_pct = None
        for record in top_gainers:
            if record["symbol"] != self.symbol:
                continue
            try:
                gain_pct = float(record["priceChangePercent"])
            except (KeyError, TypeError, ValueError):
                return None
            break

        if gain_pct is None:
            return None

        algo = "top_gainers_reversal_drop"
        bot_strategy = Position.short
        autotrade, autotrade_route = self.regime_routing()
        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)

        base_asset = self.current_symbol_data["base_asset"]
        score = float(row["reversal_drop_score"])
        score_threshold = (
            float(row["score_threshold"])
            if row["score_threshold"] == row["score_threshold"]
            else 0.0
        )
        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        daily_gain_line = (
            f"\n            - Daily gain: {round_numbers(gain_pct, 2)}%"
            if gain_pct is not None
            else ""
        )
        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: SHORT ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: SELL after a pumped 5m move shows reversal, retrace, and exhaustion
            - Market regime: {context.market_regime if context is not None and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Pump return ({self.pump_lookback} bars): {round_numbers(float(row["pump_return"]) * 100, 2)}%{daily_gain_line}
            - Retrace from peak: {round_numbers(float(row["retrace_from_peak"]) * 100, 2)}%
            - Upper wick fraction: {round_numbers(float(row["upper_wick_frac"]) * 100, 2)}%
            - Volume ratio: {round_numbers(float(row["volume_ratio"]), 2)}
            - Score: {round_numbers(score, 4)}
            - Dynamic score threshold: {round_numbers(score_threshold, 4)}
            - Volume: {round_numbers(float(row["volume"]), decimals=self.price_precision)} {base_asset}
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
                name=algo,
                position=bot_strategy,
                market_type=MarketType.FUTURES,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

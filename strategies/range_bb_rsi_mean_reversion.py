import logging
from os import getenv
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
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class RangeBbRsiMeanReversion:
    """
    Fade Bollinger Band extremes only when the market and symbol are ranging.

    Entry conditions:
        - market_regime == RANGE and symbol micro_regime == RANGE
        - market stress and symbol volatility are contained
        - ADX is low enough to avoid trend-day fades
        - price rejects the lower/upper Bollinger Band
        - RSI and Z-score confirm an oversold/overbought extreme

    Autotrade is disabled initially so the strategy can collect live telemetry
    before it is allowed to place range-fade orders.
    """

    ALGO = "range_bb_rsi_mean_reversion"
    MIN_CANDLES = 40
    ADX_WINDOW = 14
    ZSCORE_WINDOW = 20
    ADX_MAX = 32.0
    LONG_RSI_MAX = 35.0
    SHORT_RSI_MIN = 65.0
    LONG_ZSCORE_MAX = -2.0
    SHORT_ZSCORE_MIN = 2.0
    BAND_TOUCH_TOLERANCE = 0.002
    MAX_MARKET_STRESS = 0.35
    MAX_SYMBOL_ATR_PCT = 0.04
    MAX_SYMBOL_BB_WIDTH = 0.08
    MIN_REJECTION_WICK_FRAC = 0.30
    LONG_CLOSE_POSITION_MIN = 0.55
    SHORT_CLOSE_POSITION_MAX = 0.45

    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.binbot_api = cls.binbot_api
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision

    @property
    def latest_market_context(self) -> LiveMarketContext | None:
        return self.ti.latest_market_context

    def regime_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"
        if context.regime_is_transitioning:
            return False, "market_transitioning"
        if context.market_stress_score >= self.MAX_MARKET_STRESS:
            return False, "market_stress_too_high"
        if context.market_regime is None:
            return False, "market_regime_unavailable"
        if context.market_regime != "RANGE":
            return False, f"market_regime_{context.market_regime.lower()}"
        if symbol_features is None:
            return False, "symbol_regime_unavailable"
        if symbol_features.micro_regime != "RANGE":
            return False, f"symbol_regime_{str(symbol_features.micro_regime).lower()}"
        if symbol_features.micro_regime_transition in {
            "BREAKOUT_UP",
            "BREAKDOWN",
            "VOLATILITY_EXPANSION",
        }:
            return (
                False,
                f"symbol_transition_{symbol_features.micro_regime_transition.lower()}",
            )
        if symbol_features.atr_pct > self.MAX_SYMBOL_ATR_PCT:
            return False, "symbol_atr_too_high"
        if symbol_features.bb_width > self.MAX_SYMBOL_BB_WIDTH:
            return False, "symbol_bb_width_too_high"
        return True, "range_bb_rsi_mean_reversion"

    @staticmethod
    def _compute_adx(df, window: int) -> float:
        highs = df["high"].astype(float)
        lows = df["low"].astype(float)
        closes = df["close"].astype(float)

        high_diff = highs.diff()
        low_diff = -lows.diff()
        plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0.0)
        minus_dm = low_diff.where((low_diff > high_diff) & (low_diff > 0), 0.0)
        previous_close = closes.shift(1)
        true_range = (
            (highs - lows)
            .to_frame("range")
            .join((highs - previous_close).abs().rename("high_gap"))
            .join((lows - previous_close).abs().rename("low_gap"))
            .max(axis=1)
        )

        atr_sum = true_range.rolling(window, min_periods=window).sum()
        plus_di = 100.0 * plus_dm.rolling(window, min_periods=window).sum() / atr_sum
        minus_di = 100.0 * minus_dm.rolling(window, min_periods=window).sum() / atr_sum
        di_total = plus_di + minus_di
        dx = (
            100.0
            * (plus_di - minus_di).abs()
            / di_total.where(di_total != 0, float("nan"))
        ).fillna(0.0)
        adx = dx.rolling(window, min_periods=window).mean().iloc[-1]
        return 100.0 if adx != adx else float(adx)

    @staticmethod
    def _compute_zscore(df, window: int) -> float:
        closes = df["close"].astype(float)
        mean = closes.rolling(window, min_periods=window).mean().iloc[-1]
        std = closes.rolling(window, min_periods=window).std(ddof=0).iloc[-1]
        if std == 0 or std != std:
            return 0.0
        return float((closes.iloc[-1] - mean) / std)

    def _bullish_rejection(self, row, bb_low: float) -> bool:
        candle_range = float(row["high"] - row["low"])
        if candle_range <= 0:
            return False
        lower_wick = float(min(row["open"], row["close"]) - row["low"])
        close_position = float((row["close"] - row["low"]) / candle_range)
        touched_lower_band = row["low"] <= bb_low * (1.0 + self.BAND_TOUCH_TOLERANCE)
        return (
            touched_lower_band
            and row["close"] > row["open"]
            and lower_wick / candle_range >= self.MIN_REJECTION_WICK_FRAC
            and close_position >= self.LONG_CLOSE_POSITION_MIN
        )

    def _bearish_rejection(self, row, bb_high: float) -> bool:
        candle_range = float(row["high"] - row["low"])
        if candle_range <= 0:
            return False
        upper_wick = float(row["high"] - max(row["open"], row["close"]))
        close_position = float((row["close"] - row["low"]) / candle_range)
        touched_upper_band = row["high"] >= bb_high * (1.0 - self.BAND_TOUCH_TOLERANCE)
        return (
            touched_upper_band
            and row["close"] < row["open"]
            and upper_wick / candle_range >= self.MIN_REJECTION_WICK_FRAC
            and close_position <= self.SHORT_CLOSE_POSITION_MAX
        )

    def _resolve_entry(
        self,
        *,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
        rsi_value: float,
        zscore: float,
    ) -> tuple[Position | None, str]:
        row = self.ti.df_15m.iloc[-1]

        if (
            current_price <= bb_mid
            and rsi_value <= self.LONG_RSI_MAX
            and zscore <= self.LONG_ZSCORE_MAX
            and self._bullish_rejection(row, bb_low)
        ):
            return Position.long, "lower_band_rsi_zscore_rejection"

        if (
            current_price >= bb_mid
            and rsi_value >= self.SHORT_RSI_MIN
            and zscore >= self.SHORT_ZSCORE_MIN
            and self._bearish_rejection(row, bb_high)
        ):
            return Position.short, "upper_band_rsi_zscore_rejection"

        return None, "no_bb_rsi_rejection_setup"

    @staticmethod
    def _direction_for_position(position: Position) -> str:
        return "LONG" if position == Position.long else "SHORT"

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        df = self.ti.df_15m.copy()
        self.ti.df_15m = df
        required_cols = ["open", "high", "low", "close", "rsi", "volume"]
        if len(df) < self.MIN_CANDLES or df[required_cols].tail(5).isnull().any().any():
            logging.info(
                "%s skipped: not enough 15m data for symbol %s",
                self.ALGO,
                self.symbol,
            )
            return

        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        should_route, route_reason = self.regime_routing(
            context=context,
            symbol_features=symbol_features,
        )
        if not should_route:
            return

        adx_value = self._compute_adx(df, self.ADX_WINDOW)
        if adx_value > self.ADX_MAX:
            return

        zscore = self._compute_zscore(df, self.ZSCORE_WINDOW)
        rsi_value = float(df["rsi"].iloc[-1])
        bot_strategy, entry_reason = self._resolve_entry(
            current_price=float(current_price),
            bb_high=float(bb_high),
            bb_mid=float(bb_mid),
            bb_low=float(bb_low),
            rsi_value=rsi_value,
            zscore=zscore,
        )
        if bot_strategy is None:
            return

        direction = self._direction_for_position(bot_strategy)
        autotrade = True
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )
        base_asset = self.current_symbol_data["base_asset"]
        quote_asset = self.current_symbol_data["quote_asset"]
        latest_row = df.iloc[-1]
        candle_range = float(latest_row["high"] - latest_row["low"])
        atr_stop_note = (
            "long stop below the rejected range edge"
            if bot_strategy == Position.long
            else "short stop above the rejected range edge"
        )

        local_score = (
            1.0
            + min(abs(zscore) / 3.0, 1.0) * 0.35
            + max(0.0, (self.ADX_MAX - adx_value) / self.ADX_MAX) * 0.25
            + (
                max(0.0, (self.LONG_RSI_MAX - rsi_value) / self.LONG_RSI_MAX)
                if bot_strategy == Position.long
                else max(0.0, (rsi_value - self.SHORT_RSI_MIN) / 35.0)
            )
            * 0.25
        )

        value = SignalsConsumer(
            direction=direction,
            autotrade=autotrade,
            current_price=current_price,
            score=local_score,
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

        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {direction} ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: fade a Bollinger Band extreme only while market and symbol context are range-bound
            - Entry setup: {entry_reason}
            - RSI: {round_numbers(rsi_value, 2)}
            - Z-score: {round_numbers(zscore, 3)}
            - ADX: {round_numbers(adx_value, 2)}
            - Candle range: {round_numbers(candle_range, decimals=self.price_precision)} {quote_asset}
            - Volume: {round_numbers(float(latest_row["volume"]), decimals=self.price_precision)} {base_asset}
            - Bollinger lower/mid/upper: {round_numbers(bb_low, decimals=self.price_precision)} / {round_numbers(bb_mid, decimals=self.price_precision)} / {round_numbers(bb_high, decimals=self.price_precision)}
            - Exit intent: take profit near mid-band first; only hold toward the opposite band if momentum stays weak
            - Stop intent: ATR/range invalidation, {atr_stop_note}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Market stress: {round_numbers(context.market_stress_score, 3) if context else "UNAVAILABLE"}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin ATR pct: {round_numbers(symbol_features.atr_pct * 100, 2) if symbol_features else "UNAVAILABLE"}%
            - Coin BB width: {round_numbers(symbol_features.bb_width * 100, 2) if symbol_features else "UNAVAILABLE"}%
            - Autotrade route: {route_reason}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        self.ti.dispatch_signal_record(
            value=value,
            indicators={
                "range_bb_rsi_adx": adx_value,
                "range_bb_rsi_zscore": zscore,
                "range_bb_rsi_entry_reason": entry_reason,
                "range_bb_rsi_autotrade_route": route_reason,
            },
        )
        self.telegram_consumer.dispatch_signal(msg)
        if autotrade:
            await self.at_consumer.process_autotrade_restrictions(value)
        logging.info(
            "[%s] %s signal emitted for %s (autotrade enabled, route=%s)",
            self.ALGO,
            direction.lower(),
            self.symbol,
            route_reason,
        )

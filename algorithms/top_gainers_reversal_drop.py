from os import getenv
from typing import TYPE_CHECKING

from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import (
    HABollinguerSpread,
    KlineSchema,
    MarketType,
    SignalsConsumer,
    Strategy,
    round_numbers,
)

from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopGainersReversalDrop:
    def __init__(self, cls: "ContextEvaluator"):
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
        self.df: TypedDataFrame[KlineSchema] = cls.df.copy()

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

    def compute_indicators(self) -> TypedDataFrame[KlineSchema]:
        df = self.df.copy()
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
        if (
            self.df is None
            or self.df.empty
            or len(self.df) < (self.lookback_window + self.pump_lookback)
        ):
            return None

        df = self.compute_indicators()
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
        bot_strategy = Strategy.margin_short
        autotrade = False
        if autotrade:
            context = self.latest_market_context
            if context is not None:
                if context.market_stress_score >= self._autotrade_stress_threshold:
                    autotrade = False
                elif context.advancers_ratio >= 0.5 + self._breadth_cross_tolerance:
                    # skip margin short if strong bullish breadth
                    return None
                elif context.advancers_ratio <= 0.5 - self._breadth_cross_tolerance:
                    autotrade = bot_strategy == Strategy.margin_short

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
            - [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Pump return ({self.pump_lookback} bars): {round_numbers(float(row["pump_return"]) * 100, 2)}%{daily_gain_line}
            - Retrace from peak: {round_numbers(float(row["retrace_from_peak"]) * 100, 2)}%
            - Upper wick fraction: {round_numbers(float(row["upper_wick_frac"]) * 100, 2)}%
            - Volume ratio: {round_numbers(float(row["volume_ratio"]), 2)}
            - Score: {round_numbers(score, 4)}
            - Dynamic score threshold: {round_numbers(score_threshold, 4)}
            - 📊 {base_asset} volume: {round_numbers(float(row["volume"]), decimals=self.price_precision)}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            msg=msg,
            symbol=self.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            market_type=MarketType.FUTURES,
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

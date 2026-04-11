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

from consumers.signal_collector import SignalCollector
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class ActivityBurstPump:
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
        self.signal_collector = SignalCollector(
            first_seen_at=cls.first_seen_at,
            interval=cls.interval,
            binbot_api=cls.binbot_api,
        )

        self.volume_multiplier = 2.75
        self.quote_volume_multiplier = 2.5
        self.price_threshold = 0.01
        self.lookback_window = 20
        self.min_baseline_volume = 1e-8
        self.min_range_frac = 0.012
        self.min_body_frac = 0.45
        self.max_close_to_high = 0.35
        self.min_recent_up_closes = 2
        self.score_quantile = 0.92
        self.score_lookback = 80
        self.cooldown_bars = 3

    def compute_indicators(self) -> TypedDataFrame[KlineSchema]:
        df = self.df.copy()
        has_quote_asset_volume = "quote_asset_volume" in df.columns

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

        if has_quote_asset_volume:
            df["baseline_quote_volume"] = (
                df["quote_asset_volume"]
                .shift(2)
                .rolling(window=baseline_window - 1, min_periods=baseline_window - 1)
                .median()
            )
            df["baseline_quote_volume_safe"] = df["baseline_quote_volume"].clip(
                lower=self.min_baseline_volume
            )
            df["quote_volume_ratio"] = (
                df["quote_asset_volume"] / df["baseline_quote_volume_safe"]
            )
        else:
            # Older spot fixtures only provide base volume. Treat quote-volume
            # confirmation as neutral instead of failing on a missing column.
            df["baseline_quote_volume"] = df["baseline_volume"]
            df["baseline_quote_volume_safe"] = df["baseline_volume_safe"]
            df["quote_volume_ratio"] = 1.0

        prev_close = df["close"].shift(1).clip(lower=self.min_baseline_volume)
        candle_range = (df["high"] - df["low"]).clip(lower=self.min_baseline_volume)
        candle_body = (df["close"] - df["open"]).abs()

        df["price_jump"] = (df["close"] - df["close"].shift(1)) / prev_close
        df["range_frac"] = candle_range / df["close"].clip(
            lower=self.min_baseline_volume
        )
        df["body_frac"] = candle_body / candle_range
        df["close_to_high"] = (df["high"] - df["close"]) / candle_range
        df["is_bullish"] = df["close"] > df["open"]
        df["recent_up_closes"] = (df["close"] > df["close"].shift(1)).rolling(3).sum()
        df["vol_spike"] = df["volume"] > (
            self.volume_multiplier * df["baseline_volume_safe"]
        )
        if has_quote_asset_volume:
            df["quote_vol_spike"] = df["quote_asset_volume"] > (
                self.quote_volume_multiplier * df["baseline_quote_volume_safe"]
            )
        else:
            df["quote_vol_spike"] = True
        df["price_jump_flag"] = df["price_jump"] > self.price_threshold
        df["range_expansion_flag"] = df["range_frac"] > self.min_range_frac
        df["body_quality_flag"] = (
            df["is_bullish"]
            & (df["body_frac"] > self.min_body_frac)
            & (df["close_to_high"] < self.max_close_to_high)
        )
        if has_quote_asset_volume:
            df["trend_quality_flag"] = (
                df["recent_up_closes"] >= self.min_recent_up_closes
            )
        else:
            df["trend_quality_flag"] = df["recent_up_closes"] >= 1
        if has_quote_asset_volume:
            df["activity_burst_score"] = (
                df["volume_ratio"]
                * df["quote_volume_ratio"]
                * df["price_jump"].clip(lower=0)
                * (1 + df["body_frac"])
            )
        else:
            df["activity_burst_score"] = df["volume_ratio"] * df["price_jump"].clip(
                lower=0
            )
        df["score_threshold"] = (
            df["activity_burst_score"]
            .shift(1)
            .rolling(self.score_lookback, min_periods=self.lookback_window)
            .quantile(self.score_quantile)
        )
        raw_signal = (
            df["vol_spike"]
            & df["quote_vol_spike"]
            & df["price_jump_flag"]
            & df["range_expansion_flag"]
            & df["body_quality_flag"]
            & df["trend_quality_flag"]
            & (df["activity_burst_score"] >= df["score_threshold"].fillna(0))
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
            or len(self.df) < (self.lookback_window + 1)
        ):
            return None

        algo = "activity_burst_pump"
        autotrade = False
        bot_strategy = Strategy.long
        if autotrade:
            context = self.latest_market_context
            if context is not None:
                if context.market_stress_score >= self._autotrade_stress_threshold:
                    autotrade = False
                elif context.advancers_ratio >= 0.5 + self._breadth_cross_tolerance:
                    autotrade = bot_strategy == Strategy.long
                elif context.advancers_ratio <= 0.5 - self._breadth_cross_tolerance:
                    autotrade = bot_strategy == Strategy.margin_short
        base_asset = self.current_symbol_data["base_asset"]

        df = self.compute_indicators()
        row = df.iloc[-1]

        if not bool(row["qualified_signal"]):
            return None

        score = float(row["activity_burst_score"])
        score_threshold = (
            float(row["score_threshold"])
            if row["score_threshold"] == row["score_threshold"]
            else 0.0
        )
        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Baseline volume: {round_numbers(float(row["baseline_volume_safe"]), decimals=self.price_precision)}
            - Volume ratio: {round_numbers(float(row["volume_ratio"]), 2)}
            - Quote volume ratio: {round_numbers(float(row["quote_volume_ratio"]), 2)}
            - Price jump: {round_numbers(float(row["price_jump"]) * 100, 2)}%
            - Range expansion: {round_numbers(float(row["range_frac"]) * 100, 2)}%
            - Candle body fraction: {round_numbers(float(row["body_frac"]) * 100, 2)}%
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

from os import getenv
from typing import TYPE_CHECKING

from models.signals import SignalCandidate
from pandera.typing import DataFrame as TypedDataFrame
from pybinbot import KlineSchema, Strategy, round_numbers

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
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.qty_precision = cls.qty_precision
        self.df: TypedDataFrame[KlineSchema] = cls.df.copy()
        self.signal_collector = SignalCollector(
            first_seen_at=cls.first_seen_at,
            interval=cls.interval,
            binbot_api=cls.binbot_api,
        )

        self.volume_multiplier = 5.0
        self.price_threshold = 0.02
        self.lookback_window = 20
        self.min_baseline_volume = 1e-8

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
        df["price_jump"] = (df["close"] - df["close"].shift(1)).abs() / df[
            "close"
        ].shift(1).clip(lower=self.min_baseline_volume)
        df["range_frac"] = (df["high"] - df["low"]) / df["close"].clip(
            lower=self.min_baseline_volume
        )
        df["vol_spike"] = df["volume"] > (
            self.volume_multiplier * df["baseline_volume_safe"]
        )
        df["price_jump_flag"] = df["price_jump"] > self.price_threshold
        df["activity_burst_score"] = df["volume_ratio"] * df["price_jump"]

        return df

    async def signal_generator(self, current_price: float) -> None:
        if (
            self.df is None
            or self.df.empty
            or len(self.df) < (self.lookback_window + 1)
        ):
            return None

        algo = "activity_burst_pump"
        autotrade = True
        bot_strategy = Strategy.long
        base_asset = self.current_symbol_data["base_asset"]

        df = self.compute_indicators()
        row = df.iloc[-1]

        if not bool(row["vol_spike"]) or not bool(row["price_jump_flag"]):
            return None

        score = float(row["activity_burst_score"])
        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Baseline volume: {round_numbers(float(row["baseline_volume_safe"]), decimals=self.price_precision)}
            - Volume ratio: {round_numbers(float(row["volume_ratio"]), 2)}
            - Price jump: {round_numbers(float(row["price_jump"]) * 100, 2)}%
            - Range expansion: {round_numbers(float(row["range_frac"]) * 100, 2)}%
            - Score: {round_numbers(score, 4)}
            - 📊 {base_asset} volume: {round_numbers(float(row["volume"]), decimals=self.price_precision)}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        value = SignalCandidate(
            symbol=self.symbol,
            algo=algo,
            strategy=bot_strategy,
            autotrade=autotrade,
            market_type=self.market_type,
            score=score,
            current_price=current_price,
            volume=float(row["volume"]),
            msg=msg,
        )

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

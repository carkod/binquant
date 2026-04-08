from os import getenv
from typing import TYPE_CHECKING
from pybinbot import (
    HABollinguerSpread,
    SignalsConsumer,
    Strategy,
    round_numbers,
    KlineSchema,
    MarketType,
)
from pandera.typing import DataFrame as TypedDataFrame
from consumers.signal_collector import SignalCollector
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LiquidationSweepPump:
    def __init__(self, cls: "ContextEvaluator"):
        self.config = cls.config
        # Symbol / context
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
        self.oi_growth = cls.oi_data
        self.df: TypedDataFrame[KlineSchema] = cls.df_15m.copy()
        self.df_btc: TypedDataFrame[KlineSchema] = cls.df_btc.copy()
        self.signal_collector = SignalCollector(
            first_seen_at=cls.first_seen_at,
            interval=cls.interval,
            binbot_api=cls.binbot_api,
        )

    def compute_pump_score(self, window_hours=3) -> TypedDataFrame[KlineSchema]:
        """
        Compute pump score using:
        - Relative volume
        - Early momentum
        - Price compression
        - OI growth (cached per asset; defaults to 1.0 if unavailable)
        """
        df = self.df.copy()

        # --- 1. Relative Volume ---
        df["rel_volume"] = df.volume / df.volume.rolling(
            window=window_hours * 2
        ).mean().shift(window_hours)

        # --- 2. Early Momentum ---
        df["price_momentum"] = df.close.pct_change(periods=window_hours)

        # --- 3. Price Compression ---
        df["price_range_frac"] = (
            df.high.rolling(window=window_hours * 2).max()
            - df.low.rolling(window=window_hours * 2).min()
        ) / df.close

        # --- 4. OI Growth ---
        oi_growth = 1 + max(0, (self.oi_growth - 1)) if self.oi_growth else 1.0

        # --- 5. Pump Score ---
        df["pump_score"] = (
            df["rel_volume"] * (1 + df["price_momentum"]) * oi_growth
        ) / df["price_range_frac"]
        df["pump_score_smooth"] = df["pump_score"].rolling(window=2).mean()

        return df

    async def signal_generator(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        """
        Generate signal if pump score exceeds threshold and OI growth filter
        """
        if self.df is None or self.df.empty:
            return None

        algo = "liquidation_sweep_pump"
        autotrade = True
        bot_strategy = Strategy.long
        base_asset = self.current_symbol_data["base_asset"]

        df = self.compute_pump_score()

        # --- Filters ---
        # Take last N candles (say 48 for 12h)
        recent_scores = df["pump_score_smooth"].iloc[-48:]
        btc_momentum = self.df_btc.close.pct_change().iloc[-1]

        # Compute dynamic threshold
        # For example, top 5% of historical scores
        PUMP_SCORE_THRESHOLD = recent_scores.quantile(0.95)
        row = df.iloc[-1]
        latest_score = row["pump_score_smooth"]

        if btc_momentum < -0.01:
            return

        if latest_score is None or float(latest_score) < PUMP_SCORE_THRESHOLD:
            return

        # Optional OI confirmation
        if self.oi_growth is not None and self.oi_growth < 1.05:
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Score: {latest_score:.2f}
            - 📊 {base_asset} volume: {round_numbers(float(row.volume), decimals=self.price_precision)}
            - OI Growth: {self.oi_growth:.2f}
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

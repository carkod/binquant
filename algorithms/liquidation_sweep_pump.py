from os import getenv
from typing import TYPE_CHECKING

from pandas import DataFrame
from models.signals import SignalCandidate
from pybinbot import Strategy, round_numbers
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
        self.df: DataFrame = cls.df.copy()
        self.btc_df = cls.btc_df.copy()
        self.signal_collector = SignalCollector(
            first_seen_at=cls.first_seen_at, interval=cls.interval
        )

    def compute_pump_score(
        self,
        oi_col="open_interest",
        vol_col="volume",
        price_col="close",
        window_hours=3,
    ) -> DataFrame:
        """
        Compute the PumpScore for a single asset based on:
        - Open Interest growth
        - Volume growth
        - Price compression

        df: pandas DataFrame with candlestick data sorted by timestamp ascending
        window_hours: number of past hours to calculate momentum

        Returns: df with a new column 'pump_score'
        """

        df = self.df.copy()

        # 1. Volume Growth
        df["vol_growth"] = df[vol_col].rolling(window=window_hours).sum() / df[
            vol_col
        ].rolling(window=window_hours * 2).sum().shift(window_hours)

        # 2. Open Interest Growth (if available)
        if oi_col in df.columns:
            df["oi_growth"] = df[oi_col].rolling(window=window_hours).mean() / df[
                oi_col
            ].rolling(window=window_hours * 2).mean().shift(window_hours)
        else:
            df["oi_growth"] = 1.0  # default to 1 if no OI

        # 3. Price Range Fraction (Price compression)
        df["price_range_frac"] = (
            df["high"].rolling(window=window_hours * 2).max()
            - df["low"].rolling(window=window_hours * 2).min()
        ) / df[price_col]

        # 4. Pump Score
        df["pump_score"] = (df["oi_growth"] * df["vol_growth"]) / df["price_range_frac"]

        # Optional: smooth the score to reduce noise
        df["pump_score_smooth"] = df["pump_score"].rolling(window=2).mean()

        return df

    async def signal_generator(self, current_price: float) -> None:
        """
        Generate a signal if the pump score exceeds a certain threshold
        """
        if self.df is None or self.df.empty:
            return None

        algo = "liquidation sweep pump"
        autotrade = False
        df = self.compute_pump_score()
        row = df.iloc[-1]
        latest_score = df["pump_score_smooth"].iloc[-1]
        base_asset = self.current_symbol_data["base_asset"]
        bot_strategy = Strategy.long

        # ignore weak signals
        PUMP_SCORE_THRESHOLD = 15

        if latest_score < PUMP_SCORE_THRESHOLD:
            return

        kucoin_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[0]
        terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )[1]

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Score: {latest_score}
            - 📊 {base_asset} volume: {round_numbers(float(row.get("volume", 0.0)), decimals=self.price_precision)}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        candidate = SignalCandidate(
            symbol=self.symbol,
            algo=algo,
            strategy=bot_strategy,
            autotrade=autotrade,
            market_type=self.market_type,
            score=latest_score,
            current_price=current_price,
            volume=float(row.get("volume", 0.0)),
            msg=msg,
        )

        await self.telegram_consumer.send_signal(msg)
        await self.signal_collector.handle(
            candidate=candidate,
            dispatch_function=self.at_consumer.process_autotrade_restrictions,
        )

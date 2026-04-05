from os import getenv
from time import time
from typing import TYPE_CHECKING
from pybinbot import SignalsConsumer, Strategy, round_numbers, KlineSchema
from pandera.typing import DataFrame as TypedDataFrame
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
        self.pending_signal_state = cls.pending_signal_state
        self.df: TypedDataFrame[KlineSchema] = cls.df.copy()
        self.df_btc: TypedDataFrame[KlineSchema] = cls.df_btc.copy()

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

    @staticmethod
    def safe_range(high: float, low: float) -> float:
        return max(high - low, 1e-9)

    def candle_signature(self, row) -> tuple[float, float, float, float]:
        return (
            round(float(row["open"]), 12),
            round(float(row["high"]), 12),
            round(float(row["low"]), 12),
            round(float(row["close"]), 12),
        )

    def build_pending_event(self, row, score: float, swing_high: float) -> dict:
        return {
            "algo": "liquidation_sweep_pump",
            "event_signature": self.candle_signature(row),
            "last_checked_signature": None,
            "event_open": float(row["open"]),
            "event_high": float(row["high"]),
            "event_low": float(row["low"]),
            "event_close": float(row["close"]),
            "event_volume": float(row["volume"]),
            "event_score": float(score),
            "swing_high": float(swing_high),
            "oi_growth": float(self.oi_growth or 1.0),
            "confirmation_checks": 0,
            "created_at": int(time() * 1000),
        }

    def confirmation_direction(self, pending: dict, row) -> str | None:
        event_high = pending["event_high"]
        event_low = pending["event_low"]
        event_open = pending["event_open"]
        event_close = pending["event_close"]
        swing_high = pending["swing_high"]

        event_range = self.safe_range(event_high, event_low)
        event_upper_wick_frac = (
            event_high - max(event_open, event_close)
        ) / event_range
        event_close_position = (event_close - event_low) / event_range

        confirm_open = float(row["open"])
        confirm_close = float(row["close"])

        bullish_confirmation = (
            event_close > swing_high
            and event_close_position >= 0.60
            and confirm_close >= swing_high * 0.998
            and confirm_close > confirm_open
        )

        bearish_confirmation = (
            event_high > swing_high
            and (
                event_upper_wick_frac >= 0.25
                or event_close <= (event_high - 0.40 * event_range)
            )
            and confirm_close < swing_high * 1.002
            and confirm_close < confirm_open
        )

        if bullish_confirmation:
            return "LONG"
        if bearish_confirmation:
            return "SHORT"
        return None

    async def signal_generator(self, current_price: float) -> None:
        """
        Detect sweep events and emit immediately.
        """
        if self.df is None or self.df.empty or len(self.df) < 8:
            return None

        df = self.compute_pump_score()
        row = df.iloc[-1]

        # --- Filters ---
        # Take last N candles (say 48 for 12h)
        recent_scores = df["pump_score_smooth"].iloc[-48:]
        btc_momentum = self.df_btc.close.pct_change().iloc[-1]

        # Compute dynamic threshold
        # For example, top 5% of historical scores
        PUMP_SCORE_THRESHOLD = recent_scores.quantile(0.95)
        latest_score = row["pump_score_smooth"]

        if btc_momentum < -0.01:
            return

        if latest_score is None or float(latest_score) < PUMP_SCORE_THRESHOLD:
            return

        # Optional OI confirmation
        if self.oi_growth is not None and self.oi_growth < 1.01:
            return

        swing_window = df["high"].iloc[-7:-1]
        swing_high = (
            float(swing_window.max()) if not swing_window.empty else float(row["high"])
        )

        if float(row["high"]) < swing_high:
            return

        event = self.build_pending_event(
            row=row,
            score=float(latest_score),
            swing_high=swing_high,
        )
        direction = self.confirmation_direction(event, row)
        if direction is None:
            direction = "LONG" if float(row["close"]) >= float(row["open"]) else "SHORT"

        await self.dispatch_confirmed_signal(
            current_price=current_price,
            latest_row=row,
            pending=event,
            direction=direction,
        )

    async def dispatch_confirmed_signal(
        self,
        current_price: float,
        latest_row,
        pending: dict,
        direction: str,
    ) -> None:
        algo = "liquidation_sweep_pump"
        autotrade = False
        bot_strategy = Strategy.margin_short if direction == "SHORT" else Strategy.long
        base_asset = self.current_symbol_data["base_asset"]
        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        msg = f"""
            - {"📈" if direction == "LONG" else "📉"} [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Direction: {direction}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Score: {pending["event_score"]:.2f}
            - Event swing high: {round_numbers(pending["swing_high"], decimals=self.price_precision)}
            - Event high: {round_numbers(pending["event_high"], decimals=self.price_precision)}
            - Event close: {round_numbers(pending["event_close"], decimals=self.price_precision)}
            - Confirm close: {round_numbers(float(latest_row["close"]), decimals=self.price_precision)}
            - 📊 {base_asset} volume: {round_numbers(float(latest_row["volume"]), decimals=self.price_precision)}
            - OI Growth: {pending["oi_growth"]:.2f}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            symbol=self.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            autotrade=autotrade,
            market_type=self.market_type,
            current_price=current_price,
            msg=msg,
        )

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

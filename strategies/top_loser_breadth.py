import logging
from math import isfinite
from time import time
from typing import TYPE_CHECKING

from pandas import to_numeric
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketBreadthSeries,
    MarketType,
    Position,
    SignalsConsumer,
    breadth_momentum_reversal,
    btc_trend_confirms,
    latest_beta,
    round_numbers,
    timestamp_sort_key,
)

from shared.utils import build_links_msg, format_context_timestamp_line
from strategies.higher_low_pattern import HigherLowPattern

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopLoserBreadth:
    """Long a current 2nd-to-11th ranked loser as bearish momentum fails.

    Exact sign-mirror of TopGainerBreadth (strategies/top_gainer_breadth.py).
    Entry requires:
    - the symbol is currently a ranked top loser (2nd-11th, by 24h move) —
      the starting filter for which symbols this strategy considers at all;
    - the symbol's own 15m candles have just confirmed a higher low. This is
      the trigger: without it nothing else here matters.

    Market-breadth momentum (fast EMA(3) crossing above its slower average
    while breadth is still extended bearish) and BTC's 15m trend are
    confirming context, not entry gates, for the same reason as the gainer
    side: requiring both to line up with the price pattern would have
    blocked real manual trades that only agreed on the price-structure
    rollover. Each one present still adds to the signal's conviction score
    (SignalsConsumer.score) and is recorded in indicators.

    The resulting futures long uses an exchange-native stop at the lower
    Bollinger Band, capped at 4%, and dynamic trailing protection. Reversal
    and recovery are explicitly disabled. Once open, binbot streaming owns
    the position lifecycle and closes it only through stop-loss or trailing
    protection.
    """

    ALGO = "top_loser_breadth"

    TOP_LOSER_RANK_START = 2
    TOP_LOSER_RANK_END = 11
    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    ENTRY_COOLDOWN_MINUTES = 60
    TRAILING_PROFIT_PCT = 3.5
    TRAILING_DEVIATION_PCT = 2.5
    MAX_STOP_LOSS_PCT = 4.0

    MIN_BREADTH_HISTORY = 12
    BREADTH_FAST_EMA_SPAN = 3
    BREADTH_EXTENSION_THRESHOLD = 0.15
    BREADTH_FLOOR = -0.6
    MAX_BREADTH_AGE_SECONDS = 30 * 60
    MAX_LOSERS_SNAPSHOT_AGE_SECONDS = 75 * 60

    MIN_BTC_HISTORY = 20
    BTC_TREND_EMA_SPAN = 20

    # ~2.5 days of 15m bars: the largest window that comfortably fits within
    # the ~300-bar history left in df_15m after indicator warm-up
    # (klines_provider.py fetches 400 raw candles; the ma_100 warm-up alone
    # consumes ~99 of them). Short of the 7-30 days a stable beta ideally
    # wants, but the most the current candle-fetch depth can support without
    # chronically reporting "insufficient history".
    BTC_BETA_WINDOW_BARS = 240

    BASE_SCORE = 1.0
    BREADTH_CONFIRMED_SCORE_BONUS = 0.5
    BTC_TREND_CONFIRMED_SCORE_BONUS = 0.5
    HIGH_CONVICTION_SCORE_BONUS = 0.25

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.market_breadth_data: MarketBreadthSeries | None = cls.market_breadth_data
        self.gainers_losers_series = cls.gainers_losers_series
        self.strategy_cooldowns = cls.strategy_cooldowns
        self._last_emitted_confirmation_open_time: int | None = None

    def _top_loser_entry(self) -> tuple[int, float] | None:
        if not self.gainers_losers_series:
            return None

        latest_snapshot = self.gainers_losers_series[0]
        if not self._timestamp_is_fresh(
            latest_snapshot.recorded_at,
            self.MAX_LOSERS_SNAPSHOT_AGE_SECONDS,
        ):
            return None
        return next(
            (
                (rank, entry.price_change_percent)
                for rank, entry in enumerate(
                    latest_snapshot.top_losers[
                        self.TOP_LOSER_RANK_START - 1 : self.TOP_LOSER_RANK_END
                    ],
                    start=self.TOP_LOSER_RANK_START,
                )
                if entry.symbol == self.symbol
            ),
            None,
        )

    @staticmethod
    def _timestamp_is_fresh(timestamp: object, max_age_seconds: int) -> bool:
        timestamp_seconds = timestamp_sort_key(timestamp)
        if timestamp_seconds is None:
            return False
        age_seconds = time() - timestamp_seconds
        return 0 <= age_seconds <= max_age_seconds

    @classmethod
    def _stop_loss_pct(cls, current_price: float, bb_low: float) -> float | None:
        if (
            not isfinite(current_price)
            or not isfinite(bb_low)
            or current_price <= 0
            or bb_low >= current_price
        ):
            return None

        stop_loss = (1 - (bb_low / current_price)) * 100
        return round_numbers(min(stop_loss, cls.MAX_STOP_LOSS_PCT), 4)

    def _fresh_higher_low(self) -> dict[str, float | int] | None:
        df = self.ti.df_15m
        if df is None or "close_time" not in df.columns:
            return None

        close_times = to_numeric(df["close_time"], errors="coerce")
        completed_candles = df.loc[close_times < time() * 1000]
        pattern = HigherLowPattern.detect(completed_candles)
        if pattern is None:
            return None

        latest_open_time = int(completed_candles["open_time"].iloc[-1])
        if pattern["confirmation_open_time"] != latest_open_time:
            return None
        return pattern

    def _already_emitted(self, confirmation_open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_confirmation_open_time == confirmation_open_time
        return (
            self.strategy_cooldowns.get((self.ALGO, self.symbol))
            == confirmation_open_time
        )

    def _mark_emitted(self, confirmation_open_time: int) -> None:
        self._last_emitted_confirmation_open_time = confirmation_open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = confirmation_open_time

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            return

        top_loser_entry = self._top_loser_entry()
        if top_loser_entry is None:
            logging.info("%s skipped: symbol_not_in_ranked_loser_window", self.ALGO)
            return
        top_loser_rank, price_change_24h = top_loser_entry

        higher_low = self._fresh_higher_low()
        if higher_low is None:
            logging.info("%s skipped: no_fresh_confirmed_higher_low", self.ALGO)
            return

        stop_loss = self._stop_loss_pct(current_price, bb_low)
        if stop_loss is None:
            logging.info("%s skipped: lower_bollinger_stop_invalid", self.ALGO)
            return
        stop_loss_price = round_numbers(
            current_price - (current_price * stop_loss / 100), self.price_precision
        )
        stop_loss_source = (
            "max_stop_loss_cap"
            if stop_loss == self.MAX_STOP_LOSS_PCT and bb_low < stop_loss_price
            else "lower_bollinger_band"
        )

        confirmation_open_time = int(higher_low["confirmation_open_time"])
        if self._already_emitted(confirmation_open_time):
            logging.info("%s skipped: higher_low_already_emitted", self.ALGO)
            return

        # Confirming context, not gates: each one is resolved once, right
        # here, into its own score bonus / indicators / message line, so
        # absence never blocks entry and never needs re-checking downstream.
        breadth_values, breadth_reason = breadth_momentum_reversal(
            self.market_breadth_data,
            direction=1,
            min_history=self.MIN_BREADTH_HISTORY,
            fast_ema_span=self.BREADTH_FAST_EMA_SPAN,
            extension_threshold=self.BREADTH_EXTENSION_THRESHOLD,
        )
        breadth_confirmed = False
        breadth_score_bonus = 0.0
        high_conviction_floor_reached = False
        breadth_indicators: dict[str, object] = {}
        breadth_line = "No"
        if breadth_values is not None and self._timestamp_is_fresh(
            breadth_values["breadth_timestamp"], self.MAX_BREADTH_AGE_SECONDS
        ):
            breadth_confirmed = True
            breadth_score_bonus = self.BREADTH_CONFIRMED_SCORE_BONUS
            high_conviction_floor_reached = (
                breadth_values["market_breadth"] <= self.BREADTH_FLOOR
            )
            breadth_indicators = {
                "breadth_reversal_reason": breadth_reason,
                **breadth_values,
            }
            breadth_line = f"Yes (market breadth {round_numbers(breadth_values['market_breadth'], 4)})"

        btc_trend = btc_trend_confirms(
            self.ti.df_btc_15m,
            direction=1,
            min_history=self.MIN_BTC_HISTORY,
            trend_ema_span=self.BTC_TREND_EMA_SPAN,
        )
        btc_uptrend_confirmed = False
        btc_score_bonus = 0.0
        btc_indicators: dict[str, object] = {}
        btc_line = "No"
        if btc_trend is not None:
            btc_close, btc_trend_ema = btc_trend
            btc_uptrend_confirmed = True
            btc_score_bonus = self.BTC_TREND_CONFIRMED_SCORE_BONUS
            btc_indicators = {
                "btc_close_15m": btc_close,
                "btc_trend_ema": btc_trend_ema,
            }
            btc_line = (
                f"Yes ({round_numbers(btc_close, self.price_precision)} / "
                f"EMA{self.BTC_TREND_EMA_SPAN} "
                f"{round_numbers(btc_trend_ema, self.price_precision)})"
            )

        score = round_numbers(
            self.BASE_SCORE
            + breadth_score_bonus
            + btc_score_bonus
            + (
                self.HIGH_CONVICTION_SCORE_BONUS
                if high_conviction_floor_reached
                else 0.0
            ),
            4,
        )

        # Informational only: how much this symbol has historically moved
        # per 1% BTC move (Cov(r_token, r_btc) / Var(r_btc)), not a gate or
        # score input.
        btc_beta = latest_beta(
            self.ti.df_15m["close"],
            self.ti.df_btc_15m["close"],
            window=self.BTC_BETA_WINDOW_BARS,
            min_periods=self.BTC_BETA_WINDOW_BARS,
        )

        fiat_order_size = round_numbers(
            self.at_consumer.autotrade_settings.base_order_size
            * self.FIAT_ORDER_SIZE_FRACTION,
            8,
        )
        quote_asset = self.current_symbol_data.quote_asset
        context = self.ti.latest_market_context
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        indicators = {
            "entry_reason": "higher_low_breakout",
            "top_loser_rank": top_loser_rank,
            "top_loser_price_change_24h_pct": price_change_24h,
            "higher_low_first_trough": higher_low["earlier_low"],
            "higher_low_second_trough": higher_low["later_low"],
            "higher_low_rise_pct": higher_low["rise_pct"],
            "higher_low_confirmation_open_time": confirmation_open_time,
            "breadth_reversal_confirmed": breadth_confirmed,
            "btc_uptrend_confirmed": btc_uptrend_confirmed,
            "breadth_floor": self.BREADTH_FLOOR,
            "high_conviction_floor_reached": high_conviction_floor_reached,
            "btc_beta": btc_beta,
            "stop_loss_source": stop_loss_source,
            "stop_loss_price_at_signal": stop_loss_price,
            "stop_loss_pct": stop_loss,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
            "protective_exit": "exchange_native_reduce_only_stop",
            **breadth_indicators,
            **btc_indicators,
        }

        value = SignalsConsumer(
            direction=Position.long.value.upper(),
            autotrade=False,
            current_price=float(current_price),
            score=score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.long,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=True,
                fiat_order_size=fiat_order_size,
                stop_loss=stop_loss,
                trailing=True,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                trailing_profit=self.TRAILING_PROFIT_PCT,
                margin_short_reversal=False,
                recovery_params=None,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.ti.finalize_signal_bot_params(value)
        assert value.bot_params is not None
        fiat_order_size = value.bot_params.fiat_order_size

        btc_beta_line = (
            f"{round_numbers(btc_beta, 4)}"
            if btc_beta is not None
            else "N/A (insufficient history)"
        )
        msg = f"""
            - [{self.config.env}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(current_price, self.price_precision)}
            - Rule intent: LONG a current 24h loser ranked 2nd-11th when price confirms a higher low; breadth reversal and BTC uptrend are confirming context, not required
            - Top-loser rank / 24h move: {top_loser_rank} / {round_numbers(price_change_24h, 2)}%
            - Higher low first / second trough: {round_numbers(higher_low["earlier_low"], self.price_precision)} / {round_numbers(higher_low["later_low"], self.price_precision)}
            - Breadth reversal confirmed: {breadth_line}
            - BTC uptrend confirmed: {btc_line}
            - High-conviction floor (<= {self.BREADTH_FLOOR}) reached: {"Yes" if high_conviction_floor_reached else "No"}
            - Beta vs BTC (~{round_numbers(self.BTC_BETA_WINDOW_BARS / 96, 1)}d): {btc_beta_line}
            {format_context_timestamp_line(context)}
            - Max margin: {fiat_order_size} {quote_asset}
            - Stop loss: {stop_loss_source} at {stop_loss_price} ({stop_loss}%)
            - Stop behavior: exchange-native reduce-only close; no reversal position
            - Trailing profit / deviation: {self.TRAILING_PROFIT_PCT}% / {self.TRAILING_DEVIATION_PCT}%
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Confidence score: {score}
            - Autotrade is disabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        try:
            await self.ti.dispatch_signal_record(value=value, indicators=indicators)
            self.telegram_consumer.dispatch_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)
        finally:
            # Mark emitted even if a later fallible step raises: the signal
            # record may already be persisted by then, and leaving this
            # unmarked would let the next tick see the same confirmation as
            # new and reprocess/duplicate it.
            self._mark_emitted(confirmation_open_time)

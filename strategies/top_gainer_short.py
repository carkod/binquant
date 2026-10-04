import logging
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
    round_numbers,
    timestamp_sort_key,
)

from shared.utils import build_links_msg, format_context_timestamp_line
from shared.weekly_structure_protection import (
    BOUNDARY_BUFFER_PCT,
    WeeklyStructureProtection,
    weekly_structure_protection,
)
from strategies.lower_high_pattern import LowerHighPattern

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopGainerShort:
    """Short a current top gainer as bullish momentum fails.

    Entry requires:
    - the symbol has remained in the current top-ten gainers for at least six
      hours — the ranking is a watchlist, rather than the entry trigger;
    - the symbol's own 15m candles have just confirmed a lower high. This is
      the trigger: without it nothing else here matters.

    A 24h gain of at least 20%, breadth falling over three hours, and BTC
    falling over three hours raise the notification's conviction score. They
    are context, not entry gates: the coin's lower high remains the required
    failure signal.

    The resulting futures short uses a stop just beyond the preceding week's
    resistance and a weekly-range-scaled static trailing stop. Reversal and
    recovery are explicitly disabled. This is a notification-only strategy;
    its parameters describe the proposed trade but do not open a bot.
    """

    ALGO = "top_gainer_short"

    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    ENTRY_COOLDOWN_MINUTES = 60

    TOP_GAINER_RANK_LIMIT = 10
    MIN_WATCH_HOURS = 6
    MAX_WATCH_SNAPSHOT_GAP_SECONDS = 90 * 60
    STRONG_GAIN_THRESHOLD_PCT = 20.0
    MACRO_LOOKBACK_HOURS = 3
    MAX_BREADTH_AGE_SECONDS = 30 * 60
    MAX_GAINERS_SNAPSHOT_AGE_SECONDS = 75 * 60

    BASE_SCORE = 1.0
    STRONG_GAIN_SCORE_BONUS = 0.5
    BREADTH_FALLING_SCORE_BONUS = 0.5
    BTC_FALLING_SCORE_BONUS = 0.5

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

    def _top_gainer_watch(self) -> dict[str, float | int] | None:
        if not self.gainers_losers_series:
            return None

        snapshots = sorted(
            self.gainers_losers_series,
            key=lambda snapshot: timestamp_sort_key(snapshot.recorded_at) or 0,
            reverse=True,
        )
        latest_snapshot = snapshots[0]
        if not self._timestamp_is_fresh(
            latest_snapshot.recorded_at,
            self.MAX_GAINERS_SNAPSHOT_AGE_SECONDS,
        ):
            return None

        current_entry = next(
            (
                (rank, entry.price_change_percent)
                for rank, entry in enumerate(latest_snapshot.top_gainers, start=1)
                if entry.symbol == self.symbol and rank <= self.TOP_GAINER_RANK_LIMIT
            ),
            None,
        )
        if current_entry is None:
            return None

        latest_timestamp = timestamp_sort_key(latest_snapshot.recorded_at)
        if latest_timestamp is None:
            return None

        first_seen_timestamp = latest_timestamp
        previous_timestamp = latest_timestamp
        highest_gain_pct = current_entry[1]
        for snapshot in snapshots[1:]:
            snapshot_timestamp = timestamp_sort_key(snapshot.recorded_at)
            if (
                snapshot_timestamp is None
                or previous_timestamp - snapshot_timestamp
                > self.MAX_WATCH_SNAPSHOT_GAP_SECONDS
            ):
                break
            entry = next(
                (
                    item
                    for rank, item in enumerate(snapshot.top_gainers, start=1)
                    if item.symbol == self.symbol and rank <= self.TOP_GAINER_RANK_LIMIT
                ),
                None,
            )
            if entry is None:
                break
            first_seen_timestamp = snapshot_timestamp
            previous_timestamp = snapshot_timestamp
            highest_gain_pct = max(highest_gain_pct, entry.price_change_percent)

        return {
            "top_gainer_rank": current_entry[0],
            "top_gainer_price_change_24h_pct": current_entry[1],
            "top_gainer_watch_started_at": int(first_seen_timestamp),
            "top_gainer_watch_hours": (latest_timestamp - first_seen_timestamp) / 3600,
            "top_gainer_watch_max_gain_24h_pct": highest_gain_pct,
        }

    @staticmethod
    def _timestamp_is_fresh(timestamp: object, max_age_seconds: int) -> bool:
        timestamp_seconds = timestamp_sort_key(timestamp)
        if timestamp_seconds is None:
            return False
        age_seconds = time() - timestamp_seconds
        return 0 <= age_seconds <= max_age_seconds

    def _fresh_lower_high(self) -> dict[str, float | int] | None:
        df = self.ti.df_15m
        if df is None or "close_time" not in df.columns:
            return None

        close_times = to_numeric(df["close_time"], errors="coerce")
        completed_candles = df.loc[close_times < time() * 1000]
        pattern = LowerHighPattern.detect(completed_candles)
        if pattern is None:
            return None

        latest_open_time = int(completed_candles["open_time"].iloc[-1])
        if pattern["confirmation_open_time"] != latest_open_time:
            return None
        return pattern

    def _breadth_falling_three_hours(self) -> dict[str, float] | None:
        market_breadth_data = self.market_breadth_data
        if market_breadth_data is None:
            return None

        samples: list[tuple[float, float]] = []
        for timestamp, value in zip(
            market_breadth_data.timestamp,
            market_breadth_data.market_breadth,
            strict=False,
        ):
            timestamp_seconds = timestamp_sort_key(timestamp)
            if timestamp_seconds is not None:
                samples.append((timestamp_seconds, float(value)))
        samples.sort(key=lambda sample: sample[0])
        if not samples:
            return None
        latest_timestamp, latest_breadth = samples[-1]
        if not self._timestamp_is_fresh(latest_timestamp, self.MAX_BREADTH_AGE_SECONDS):
            return None

        target_timestamp = latest_timestamp - self.MACRO_LOOKBACK_HOURS * 3600
        prior_sample = next(
            (
                sample
                for sample in reversed(samples[:-1])
                if sample[0] <= target_timestamp
            ),
            None,
        )
        if prior_sample is None:
            return None
        prior_timestamp, prior_breadth = prior_sample
        return {
            "breadth_latest": latest_breadth,
            "breadth_three_hours_ago": prior_breadth,
            "breadth_change_three_hours": latest_breadth - prior_breadth,
            "breadth_three_hours_ago_timestamp": prior_timestamp,
        }

    def _btc_falling_three_hours(self) -> dict[str, float] | None:
        df = self.ti.df_btc_15m
        if df is None or "close" not in df.columns:
            return None
        completed_candles = df
        if "close_time" in df.columns:
            close_times = to_numeric(df["close_time"], errors="coerce")
            completed_candles = df.loc[close_times < time() * 1000]
        required_candles = self.MACRO_LOOKBACK_HOURS * 4 + 1
        if len(completed_candles) < required_candles:
            return None

        btc_close = float(completed_candles["close"].iloc[-1])
        btc_three_hours_ago = float(completed_candles["close"].iloc[-required_candles])
        if btc_close <= 0 or btc_three_hours_ago <= 0:
            return None
        return {
            "btc_close_15m": btc_close,
            "btc_close_three_hours_ago": btc_three_hours_ago,
            "btc_change_three_hours_pct": (btc_close / btc_three_hours_ago - 1) * 100,
        }

    def _weekly_protection(
        self, current_price: float
    ) -> WeeklyStructureProtection | None:
        df = self.ti.df_1h
        if df is None or "close_time" not in df.columns:
            return None

        close_times = to_numeric(df["close_time"], errors="coerce")
        completed_candles = df.loc[close_times < time() * 1000]
        return weekly_structure_protection(
            completed_candles,
            current_price=current_price,
            position=Position.short,
            price_precision=self.price_precision,
        )

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

        top_gainer_watch = self._top_gainer_watch()
        if top_gainer_watch is None:
            logging.info("%s skipped: symbol_not_in_ranked_gainer_window", self.ALGO)
            return
        watch_hours = top_gainer_watch["top_gainer_watch_hours"]
        if watch_hours < self.MIN_WATCH_HOURS:
            logging.info("%s skipped: top_gainer_watch_under_six_hours", self.ALGO)
            return

        lower_high = self._fresh_lower_high()
        if lower_high is None:
            logging.info("%s skipped: no_fresh_confirmed_lower_high", self.ALGO)
            return

        protection = self._weekly_protection(current_price)
        if protection is None:
            logging.info("%s skipped: weekly_structure_protection_invalid", self.ALGO)
            return

        confirmation_open_time = int(lower_high["confirmation_open_time"])
        if self._already_emitted(confirmation_open_time):
            logging.info("%s skipped: lower_high_already_emitted", self.ALGO)
            return

        strong_gainer = (
            top_gainer_watch["top_gainer_watch_max_gain_24h_pct"]
            >= self.STRONG_GAIN_THRESHOLD_PCT
        )
        breadth_context = self._breadth_falling_three_hours()
        breadth_falling = (
            breadth_context is not None
            and breadth_context["breadth_change_three_hours"] < 0
        )
        btc_context = self._btc_falling_three_hours()
        btc_falling = (
            btc_context is not None and btc_context["btc_change_three_hours_pct"] < 0
        )

        score = round_numbers(
            self.BASE_SCORE
            + (self.STRONG_GAIN_SCORE_BONUS if strong_gainer else 0.0)
            + (self.BREADTH_FALLING_SCORE_BONUS if breadth_falling else 0.0)
            + (self.BTC_FALLING_SCORE_BONUS if btc_falling else 0.0),
            4,
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
            "entry_reason": "lower_high_breakdown",
            **top_gainer_watch,
            "strong_gainer": strong_gainer,
            "strong_gainer_threshold_pct": self.STRONG_GAIN_THRESHOLD_PCT,
            "lower_high_first_peak": lower_high["earlier_high"],
            "lower_high_second_peak": lower_high["later_high"],
            "lower_high_drop_pct": lower_high["drop_pct"],
            "lower_high_confirmation_open_time": confirmation_open_time,
            "breadth_falling_three_hours": breadth_falling,
            "btc_falling_three_hours": btc_falling,
            "weekly_resistance": protection.resistance,
            "weekly_support": protection.support,
            "weekly_structure_candles": protection.candle_count,
            "weekly_boundary_buffer_pct": BOUNDARY_BUFFER_PCT,
            "stop_loss_source": "weekly_resistance",
            "stop_loss_price_at_signal": protection.stop_loss_price,
            "stop_loss_pct": protection.stop_loss_pct,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": protection.trailing_profit_pct,
            "trailing_deviation_pct": protection.trailing_deviation_pct,
            "protective_exit": "exchange_native_reduce_only_stop",
            **(breadth_context or {}),
            **(btc_context or {}),
        }

        value = SignalsConsumer(
            direction=Position.short.value.upper(),
            autotrade=False,
            current_price=float(current_price),
            score=score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.short,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=False,
                fiat_order_size=fiat_order_size,
                stop_loss=protection.stop_loss_pct,
                trailing=True,
                trailing_deviation=protection.trailing_deviation_pct,
                trailing_profit=protection.trailing_profit_pct,
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

        msg = f"""
            - [{self.config.env}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: SHORT ENTRY
            - Current price: {round_numbers(current_price, self.price_precision)}
            - Rule intent: SHORT a watched 24h top gainer when price confirms a lower high; BTC and breadth deterioration increase priority
            - Top-gainer rank / 24h move: {top_gainer_watch["top_gainer_rank"]} / {round_numbers(top_gainer_watch["top_gainer_price_change_24h_pct"], 2)}%
            - Continuous top-10 watch: {round_numbers(watch_hours, 2)}h; maximum 24h gain: {round_numbers(top_gainer_watch["top_gainer_watch_max_gain_24h_pct"], 2)}%
            - Strong-gainer threshold (>= {self.STRONG_GAIN_THRESHOLD_PCT}%): {"Yes" if strong_gainer else "No"}
            - Lower high first / second peak: {round_numbers(lower_high["earlier_high"], self.price_precision)} / {round_numbers(lower_high["later_high"], self.price_precision)}
            - Breadth falling over 3h: {"Yes" if breadth_falling else "No"}
            - BTC falling over 3h: {"Yes" if btc_falling else "No"}
            {format_context_timestamp_line(context)}
            {self.ti.regime_telegram_lines()}
            - Max margin: {fiat_order_size} {quote_asset}
            - Weekly resistance / support ({protection.candle_count} completed 1h candles): {protection.resistance} / {protection.support}
            - Stop loss: {BOUNDARY_BUFFER_PCT}% above weekly resistance at {protection.stop_loss_price} ({protection.stop_loss_pct}%)
            - Stop behavior: exchange-native reduce-only close; no reversal position
            - Trailing stop: arms after {protection.trailing_profit_pct}% profit with {protection.trailing_deviation_pct}% deviation
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Confidence score: {score}
            - Autotrade is disabled; notification only
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        try:
            await self.ti.dispatch_signal_record(value=value, indicators=indicators)
            await self.telegram_consumer.dispatch_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)
        finally:
            # Mark emitted even if a later fallible step raises: the signal
            # record may already be persisted by then, and leaving this
            # unmarked would let the next tick see the same confirmation as
            # new and reprocess/duplicate it.
            self._mark_emitted(confirmation_open_time)

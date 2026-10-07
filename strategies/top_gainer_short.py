import logging
from decimal import ROUND_CEILING, Decimal
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
    round_numbers,
    timestamp_sort_key,
)

from shared.utils import build_links_msg, format_context_timestamp_line
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

    Breadth must also have a confirmed higher low in its latest 40 samples
    (about ten hours at the ingestion cadence). Compare the latest two strict
    troughs, each confirmed by two readings on either side. The second must
    be higher and remain unbroken. Breadth must be fresh and continuous.
    This deliberately combines a failing symbol with recovering breadth.

    A 24h gain of at least 20% raises the notification's conviction score,
    as does a stretch score, which measures how far the move has run beyond
    the coin's own daily history: a 24h gain larger than any prior daily gain, a weekly
    high above every earlier daily high, and a weekly high far above the
    earlier median close. These are context, not entry gates, and the stretch
    score is intended as the groundwork for position sizing: the coin's lower
    high remains the required failure signal.

    Three or more close-to-close crossings of the current price over the last
    24 completed 15m candles block entry as chop. Exact touches are ignored.
    The resulting futures short uses a stop above the confirmed lower high
    and a fixed static trailing stop. Reversal and
    recovery are explicitly disabled. The strategy requests autotrade in
    staging only; development and production remain notification-only.
    """

    ALGO = "top_gainer_short"

    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    ENTRY_COOLDOWN_MINUTES = 60
    CHOP_LOOKBACK_BARS = 24
    CANDLE_INTERVAL_MS = 15 * 60 * 1000
    CHOP_CROSSING_THRESHOLD = 3
    LOWER_HIGH_STOP_BUFFER_PCT = 0.25
    TRAILING_PROFIT_PCT = 4.5
    TRAILING_DEVIATION_PCT = 3.0

    TOP_GAINER_RANK_LIMIT = 10
    MIN_WATCH_HOURS = 6
    MAX_WATCH_SNAPSHOT_GAP_SECONDS = 90 * 60
    STRONG_GAIN_THRESHOLD_PCT = 20.0
    BREADTH_LOOKBACK_SAMPLES = 40
    BREADTH_FRACTAL_WING = 2
    MAX_BREADTH_AGE_SECONDS = 30 * 60
    MAX_GAINERS_SNAPSHOT_AGE_SECONDS = 75 * 60

    BASE_SCORE = 1.0
    STRONG_GAIN_SCORE_BONUS = 0.5

    STRETCH_RECENT_DAYS = 7
    STRETCH_MIN_PRIOR_DAILY_CANDLES = 14
    STRETCH_EXTENSION_THRESHOLD_PCT = 100.0
    RECORD_GAIN_SCORE_BONUS = 0.5
    NEW_HIGH_SCORE_BONUS = 0.5
    EXTENSION_SCORE_BONUS = 0.5

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

    def _breadth_higher_low(self) -> dict[str, float] | None:
        """Confirm structure on the signed breadth index, not price percentages.

        A higher low remains valid after confirmation until breached or replaced
        by a newer pair of confirmed troughs within the lookback window.
        """
        breadth = self.market_breadth_data
        if breadth is None or len(breadth.timestamp) != len(breadth.market_breadth):
            return None

        samples: list[tuple[float, float]] = []
        for timestamp, value in zip(
            breadth.timestamp, breadth.market_breadth, strict=True
        ):
            timestamp_seconds = timestamp_sort_key(timestamp)
            if timestamp_seconds is None or not isfinite(timestamp_seconds):
                return None
            samples.append((timestamp_seconds, value))
        samples.sort(key=lambda sample: sample[0])
        if len(samples) < self.BREADTH_LOOKBACK_SAMPLES:
            return None
        samples = samples[-self.BREADTH_LOOKBACK_SAMPLES :]
        latest_timestamp, latest_breadth = samples[-1]
        if not self._timestamp_is_fresh(latest_timestamp, self.MAX_BREADTH_AGE_SECONDS):
            return None
        if any(not isfinite(value) for _, value in samples):
            return None
        if any(
            not 0 < later[0] - earlier[0] <= self.MAX_BREADTH_AGE_SECONDS
            for earlier, later in zip(samples, samples[1:], strict=False)
        ):
            return None

        wing = self.BREADTH_FRACTAL_WING
        lows = [
            i
            for i in range(wing, len(samples) - wing)
            if all(
                samples[i][1] < samples[j][1]
                for j in range(i - wing, i + wing + 1)
                if i != j
            )
        ]
        if len(lows) < 2:
            return None
        earlier_pos, later_pos = lows[-2:]
        earlier_timestamp, earlier_low = samples[earlier_pos]
        later_timestamp, later_low = samples[later_pos]
        # Breadth crosses zero, so compare index values directly. Percentage
        # ratios would invert negative troughs and fail at zero.
        if later_low <= earlier_low or any(
            value <= later_low for _, value in samples[later_pos + 1 :]
        ):
            return None
        return {
            "breadth_latest": latest_breadth,
            "breadth_timestamp": latest_timestamp,
            "breadth_higher_low_first_trough": earlier_low,
            "breadth_higher_low_second_trough": later_low,
            "breadth_higher_low_rise": later_low - earlier_low,
            "breadth_higher_low_first_timestamp": earlier_timestamp,
            "breadth_higher_low_second_timestamp": later_timestamp,
            "breadth_higher_low_confirmation_timestamp": samples[later_pos + wing][0],
        }

    def _price_crossings_six_hours(self, current_price: float) -> int | None:
        """Count changes of side around a fixed current-price reference.

        Require the 24 consecutive completed 15m bars immediately before the
        current bar. Wicks alone and closes equal to the reference are not
        crossings; equal closes preserve the last observed side.
        """
        df = self.ti.df_15m
        if (
            not isfinite(current_price)
            or current_price <= 0
            or df is None
            or not {"open_time", "close_time", "close"}.issubset(df.columns)
        ):
            return None

        now_ms = time() * 1000
        window_end = int(now_ms // self.CANDLE_INTERVAL_MS) * self.CANDLE_INTERVAL_MS
        window_start = window_end - self.CHOP_LOOKBACK_BARS * self.CANDLE_INTERVAL_MS
        open_times = to_numeric(df["open_time"], errors="coerce")
        close_times = to_numeric(df["close_time"], errors="coerce")
        in_window = (open_times >= window_start) & (open_times < window_end)
        window = df.loc[in_window]
        if open_times.loc[in_window].tolist() != list(
            range(window_start, window_end, self.CANDLE_INTERVAL_MS)
        ):
            return None
        window_close_times = close_times.loc[in_window]
        if not (
            (window_close_times < now_ms)
            & (window_close_times >= open_times.loc[in_window])
            & (
                window_close_times
                <= open_times.loc[in_window] + self.CANDLE_INTERVAL_MS
            )
        ).all():
            return None
        closes = to_numeric(window["close"], errors="coerce")
        if not closes.map(isfinite).all() or (closes <= 0).any():
            return None

        previous_side = 0
        crossings = 0
        for close in closes:
            side = 1 if close > current_price else -1 if close < current_price else 0
            if side == 0:
                continue
            if previous_side and side != previous_side:
                crossings += 1
            previous_side = side
        return crossings

    def _stretch_context(
        self, max_gain_24h_pct: float, weekly_resistance: float
    ) -> dict[str, float | bool] | None:
        """Compare the current move with the coin's earlier daily history.

        "Prior" candles are completed daily candles that closed before the
        recent window, so this week's pump is not compared with itself.
        Returns None when there are too few prior daily candles: a fresh
        listing has no meaningful history to be stretched against.
        """
        df = self.ti.df_1d
        required_columns = {"open", "high", "close", "close_time"}
        if df is None or not required_columns.issubset(df.columns):
            return None

        now_ms = time() * 1000
        recent_cutoff_ms = now_ms - self.STRETCH_RECENT_DAYS * 24 * 3600 * 1000
        close_times = to_numeric(df["close_time"], errors="coerce")
        prior = df.loc[close_times < recent_cutoff_ms]
        if len(prior) < self.STRETCH_MIN_PRIOR_DAILY_CANDLES:
            return None

        opens = to_numeric(prior["open"], errors="coerce")
        highs = to_numeric(prior["high"], errors="coerce")
        closes = to_numeric(prior["close"], errors="coerce")
        prior_high = float(highs.max())
        prior_median_close = float(closes.median())
        prior_best_daily_gain_pct = float(((highs / opens - 1) * 100).max())
        if (
            not isfinite(prior_high)
            or not isfinite(prior_median_close)
            or not isfinite(prior_best_daily_gain_pct)
            or prior_median_close <= 0
        ):
            return None

        extension_pct = (weekly_resistance / prior_median_close - 1) * 100
        return {
            "stretch_prior_daily_candles": len(prior),
            "stretch_prior_best_daily_gain_pct": prior_best_daily_gain_pct,
            "stretch_prior_high": prior_high,
            "stretch_prior_median_close": prior_median_close,
            "stretch_extension_pct": extension_pct,
            "stretch_record_gain": max_gain_24h_pct > prior_best_daily_gain_pct,
            "stretch_new_high": weekly_resistance > prior_high,
            "stretch_extended": extension_pct >= self.STRETCH_EXTENSION_THRESHOLD_PCT,
        }

    def _stretch_telegram_line(
        self,
        stretch: dict[str, float | bool] | None,
        stretch_score: float,
        max_gain_24h_pct: float,
    ) -> str:
        if stretch is None:
            return "- Stretch score: n/a (under 14 prior daily candles)"
        return (
            f"- Stretch score: +{stretch_score} of {self.RECORD_GAIN_SCORE_BONUS + self.NEW_HIGH_SCORE_BONUS + self.EXTENSION_SCORE_BONUS}"
            f" | record 24h gain: {'Yes' if stretch['stretch_record_gain'] else 'No'}"
            f" ({round_numbers(max_gain_24h_pct, 2)}% vs prior best daily {round_numbers(stretch['stretch_prior_best_daily_gain_pct'], 2)}%)"
            f" | weekly high above all prior daily highs: {'Yes' if stretch['stretch_new_high'] else 'No'}"
            f" | weekly high vs prior median close: +{round_numbers(stretch['stretch_extension_pct'], 1)}%"
            f" ({'>=' if stretch['stretch_extended'] else '<'} {self.STRETCH_EXTENSION_THRESHOLD_PCT}%)"
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

        breadth_context = self._breadth_higher_low()
        if breadth_context is None:
            logging.info("%s skipped: no_valid_breadth_higher_low", self.ALGO)
            return

        crossings = self._price_crossings_six_hours(current_price)
        if crossings is None:
            logging.info("%s skipped: six_hour_candle_history_invalid", self.ALGO)
            return
        if crossings >= self.CHOP_CROSSING_THRESHOLD:
            logging.info(
                "%s skipped: choppy_current_price_crossings=%s", self.ALGO, crossings
            )
            return

        lower_high_price = float(lower_high["later_high"])
        if not isfinite(lower_high_price) or current_price >= lower_high_price:
            logging.info("%s skipped: lower_high_already_reclaimed", self.ALGO)
            return
        # Round a short stop upward so price precision cannot move it back
        # inside the pattern. Round its percentage upward for the same reason.
        stop_price = (
            Decimal(str(lower_high_price))
            * (1 + Decimal(str(self.LOWER_HIGH_STOP_BUFFER_PCT)) / 100)
        ).quantize(Decimal(1).scaleb(-self.price_precision), rounding=ROUND_CEILING)
        stop_loss_price = float(stop_price)
        stop_loss_pct = float(
            ((stop_price / Decimal(str(current_price)) - 1) * 100).quantize(
                Decimal("0.0001"), rounding=ROUND_CEILING
            )
        )
        if not 0 < stop_loss_pct <= 101:
            logging.info("%s skipped: lower_high_stop_invalid", self.ALGO)
            return

        confirmation_open_time = int(lower_high["confirmation_open_time"])
        if self._already_emitted(confirmation_open_time):
            logging.info("%s skipped: lower_high_already_emitted", self.ALGO)
            return

        strong_gainer = (
            top_gainer_watch["top_gainer_watch_max_gain_24h_pct"]
            >= self.STRONG_GAIN_THRESHOLD_PCT
        )
        stretch = self._stretch_context(
            top_gainer_watch["top_gainer_watch_max_gain_24h_pct"],
            lower_high_price,
        )
        stretch_score = (
            (self.RECORD_GAIN_SCORE_BONUS if stretch["stretch_record_gain"] else 0.0)
            + (self.NEW_HIGH_SCORE_BONUS if stretch["stretch_new_high"] else 0.0)
            + (self.EXTENSION_SCORE_BONUS if stretch["stretch_extended"] else 0.0)
            if stretch is not None
            else 0.0
        )

        score = round_numbers(
            self.BASE_SCORE
            + (self.STRONG_GAIN_SCORE_BONUS if strong_gainer else 0.0)
            + stretch_score,
            4,
        )

        fiat_order_size = round_numbers(
            self.at_consumer.autotrade_settings.base_order_size
            * self.FIAT_ORDER_SIZE_FRACTION,
            8,
        )
        quote_asset = self.current_symbol_data.quote_asset
        context = self.ti.latest_market_context
        autotrade_enabled = self.config.env.lower() == "staging"
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
            "breadth_higher_low_confirmed": True,
            "price_crossings_six_hours": crossings,
            "chop_crossing_threshold": self.CHOP_CROSSING_THRESHOLD,
            "chop_lookback_bars": self.CHOP_LOOKBACK_BARS,
            "lower_high_stop_buffer_pct": self.LOWER_HIGH_STOP_BUFFER_PCT,
            "stop_loss_source": "lower_high",
            "stop_loss_price_at_signal": stop_loss_price,
            "stop_loss_pct": stop_loss_pct,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
            "protective_exit": "exchange_native_reduce_only_stop",
            **breadth_context,
        }

        value = SignalsConsumer(
            direction=Position.short.value.upper(),
            autotrade=autotrade_enabled,
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
                stop_loss=stop_loss_pct,
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

        msg = f"""
            - [{self.config.env}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: SHORT ENTRY
            - Current price: {round_numbers(current_price, self.price_precision)}
            - Rule intent: SHORT a watched 24h top gainer when price confirms a lower high and the breadth index has a confirmed higher low
            - Top-gainer rank / 24h move: {top_gainer_watch["top_gainer_rank"]} / {round_numbers(top_gainer_watch["top_gainer_price_change_24h_pct"], 2)}%
            - Continuous top-10 watch: {round_numbers(watch_hours, 2)}h; maximum 24h gain: {round_numbers(top_gainer_watch["top_gainer_watch_max_gain_24h_pct"], 2)}%
            - Strong-gainer threshold (>= {self.STRONG_GAIN_THRESHOLD_PCT}%): {"Yes" if strong_gainer else "No"}
            - Lower high first / second peak: {round_numbers(lower_high["earlier_high"], self.price_precision)} / {round_numbers(lower_high["later_high"], self.price_precision)}
            - Breadth higher low confirmed: {round_numbers(breadth_context["breadth_higher_low_first_trough"], 4)} -> {round_numbers(breadth_context["breadth_higher_low_second_trough"], 4)}; latest index: {round_numbers(breadth_context["breadth_latest"], 4)}
            {self._stretch_telegram_line(stretch, stretch_score, top_gainer_watch["top_gainer_watch_max_gain_24h_pct"])}
            {format_context_timestamp_line(context)}
            {self.ti.regime_telegram_lines()}
            - Max margin: {fiat_order_size} {quote_asset}
            - Current-price crossings over 6h: {crossings}; blocked at {self.CHOP_CROSSING_THRESHOLD}
            - Stop loss: {self.LOWER_HIGH_STOP_BUFFER_PCT}% above lower high at {stop_loss_price} ({stop_loss_pct}%)
            - Stop behavior: exchange-native reduce-only close; no reversal position
            - Trailing stop: arms after {self.TRAILING_PROFIT_PCT}% profit with {self.TRAILING_DEVIATION_PCT}% deviation
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Confidence score: {score}
            - Autotrade: {"enabled for staging" if autotrade_enabled else "disabled; notification only"}
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

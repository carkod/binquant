import logging
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal
from math import floor, isfinite
from time import time
from typing import TYPE_CHECKING, Any

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
from pybinbot.shared.price_crossings import (
    CHOP_CROSSING_THRESHOLD,
    CHOP_CROSSING_SCORE_PENALTY,
    CHOP_LOOKBACK_HOURS,
    price_crossings_six_hours,
)

from shared.higher_low import confirmed_higher_low
from shared.utils import build_links_msg, format_context_timestamp_line
from strategies.lower_high_pattern import LowerHighPattern

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopGainerShort:
    """Short a fresh 15m lower high when BTC or breadth confirms a higher low.

    Reject at three current-price crossings over six hours, including wicks.
    Top-10 gains above 20%, strong gains, and historical stretch boost the score; crossings
    penalize it. The final score sets leverage from 1x to 5x.

    Use dynamic position sizing with a stop above the lower high. Scale initial
    margin to the original stop-risk budget; later adjustments change exposure.
    Autotrade runs in staging and production, with recovery disabled.
    """

    ALGO = "top_gainer_short"

    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    ENTRY_COOLDOWN_MINUTES = 60
    LOWER_HIGH_STOP_BUFFER_PCT = 2.0
    RISK_BUDGET_STOP_BUFFER_PCT = 0.25
    TRAILING_PROFIT_PCT = 4.5

    TOP_GAINER_RANK_LIMIT = 10
    MIN_TOP_GAINER_GAIN_PCT = 20.0
    MAX_WATCH_SNAPSHOT_GAP_SECONDS = 90 * 60
    STRONG_GAIN_THRESHOLD_PCT = 20.0
    HIGHER_LOW_LOOKBACK_SAMPLES = 40
    HIGHER_LOW_FRACTAL_WING = 2
    MAX_HIGHER_LOW_AGE_SECONDS = 30 * 60
    MAX_GAINERS_SNAPSHOT_AGE_SECONDS = 75 * 60

    BASE_SCORE = 1.0
    TOP_GAINER_SCORE_BONUS = 2.0
    STRONG_GAIN_SCORE_BONUS = 0.5

    STRETCH_RECENT_DAYS = 7
    STRETCH_MIN_PRIOR_DAILY_CANDLES = 14
    STRETCH_EXTENSION_THRESHOLD_PCT = 100.0
    RECORD_GAIN_SCORE_BONUS = 0.5
    NEW_HIGH_SCORE_BONUS = 0.5
    EXTENSION_SCORE_BONUS = 0.5

    MIN_SCORE = BASE_SCORE - (CHOP_CROSSING_THRESHOLD - 1) * CHOP_CROSSING_SCORE_PENALTY
    MAX_SCORE = (
        BASE_SCORE
        + TOP_GAINER_SCORE_BONUS
        + STRONG_GAIN_SCORE_BONUS
        + RECORD_GAIN_SCORE_BONUS
        + NEW_HIGH_SCORE_BONUS
        + EXTENSION_SCORE_BONUS
    )
    MIN_LEVERAGE = 1
    MAX_LEVERAGE = 5

    @classmethod
    def futures_leverage_for_score(cls, score: float) -> int:
        """Scale the attainable score range to whole leverage levels, ties up."""
        bounded_score = min(cls.MAX_SCORE, max(cls.MIN_SCORE, score))
        scaled = cls.MIN_LEVERAGE + (
            (bounded_score - cls.MIN_SCORE)
            / (cls.MAX_SCORE - cls.MIN_SCORE)
            * (cls.MAX_LEVERAGE - cls.MIN_LEVERAGE)
        )
        return floor(scaled + 0.5)

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.candlestick_interval = cls.candlestick_interval
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
        if (
            current_entry is None
            or not isfinite(current_entry[1])
            or current_entry[1] <= self.MIN_TOP_GAINER_GAIN_PCT
        ):
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
        """Confirm an unbroken higher low on the signed breadth index."""
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
        pattern = confirmed_higher_low(
            samples,
            now_seconds=time(),
            lookback_samples=self.HIGHER_LOW_LOOKBACK_SAMPLES,
            fractal_wing=self.HIGHER_LOW_FRACTAL_WING,
            max_age_seconds=self.MAX_HIGHER_LOW_AGE_SECONDS,
            max_gap_seconds=self.MAX_HIGHER_LOW_AGE_SECONDS,
        )
        if pattern is None:
            return None
        return {f"breadth_{name}": value for name, value in pattern.items()}

    def _btc_higher_low(self) -> dict[str, float] | None:
        df = self.ti.df_btc_15m
        if df is None or not {"low", "close_time"}.issubset(df.columns):
            return None
        now_seconds = time()
        close_times = to_numeric(df["close_time"], errors="coerce")
        if not close_times.map(isfinite).all():
            return None
        completed = close_times < now_seconds * 1000
        lows = to_numeric(df.loc[completed, "low"], errors="coerce")
        samples = [
            (float(timestamp) / 1000, float(low))
            for timestamp, low in zip(close_times.loc[completed], lows, strict=True)
        ]
        pattern = confirmed_higher_low(
            samples,
            now_seconds=now_seconds,
            lookback_samples=self.HIGHER_LOW_LOOKBACK_SAMPLES,
            fractal_wing=self.HIGHER_LOW_FRACTAL_WING,
            max_age_seconds=self.MAX_HIGHER_LOW_AGE_SECONDS,
            max_gap_seconds=15 * 60,
        )
        if pattern is None or any(
            low <= 0 for _, low in sorted(samples)[-self.HIGHER_LOW_LOOKBACK_SAMPLES :]
        ):
            return None
        return {f"btc_{name}": value for name, value in pattern.items()}

    def _stretch_context(
        self, max_gain_24h_pct: float | None, weekly_resistance: float
    ) -> dict[str, float | bool] | None:
        """Compare the recent move with daily candles preceding that window."""
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
            "stretch_record_gain": (
                max_gain_24h_pct is not None
                and max_gain_24h_pct > prior_best_daily_gain_pct
            ),
            "stretch_new_high": weekly_resistance > prior_high,
            "stretch_extended": extension_pct >= self.STRETCH_EXTENSION_THRESHOLD_PCT,
        }

    def _stretch_telegram_line(
        self,
        stretch: dict[str, float | bool] | None,
        stretch_score: float,
        max_gain_24h_pct: float | None,
    ) -> str:
        if stretch is None:
            return "- Stretch score: n/a (under 14 prior daily candles)"
        record_gain = (
            f"{'Yes' if stretch['stretch_record_gain'] else 'No'}"
            f" ({round_numbers(max_gain_24h_pct, 2)}% vs prior best daily {round_numbers(stretch['stretch_prior_best_daily_gain_pct'], 2)}%)"
            if max_gain_24h_pct is not None
            else "n/a (no fresh top-10 gain data)"
        )
        return (
            f"- Stretch score: +{stretch_score} of {self.RECORD_GAIN_SCORE_BONUS + self.NEW_HIGH_SCORE_BONUS + self.EXTENSION_SCORE_BONUS}"
            f" | record 24h gain: {record_gain}"
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

        lower_high = self._fresh_lower_high()
        if lower_high is None:
            logging.info("%s skipped: no_fresh_confirmed_lower_high", self.ALGO)
            return

        breadth_context = self._breadth_higher_low()
        btc_context = self._btc_higher_low()
        if breadth_context is None and btc_context is None:
            logging.info("%s skipped: no_valid_btc_or_breadth_higher_low", self.ALGO)
            return

        crossings = price_crossings_six_hours(
            self.ti.df_15m,
            current_price,
            now_ms=time() * 1000,
            interval_ms=self.candlestick_interval.get_ms(),
        )
        if crossings is None:
            logging.info("%s skipped: six_hour_candle_history_invalid", self.ALGO)
            return
        if crossings >= CHOP_CROSSING_THRESHOLD:
            logging.info(
                "%s skipped: choppy_current_price_crossings=%s", self.ALGO, crossings
            )
            return

        lower_high_price = float(lower_high["later_high"])
        if not isfinite(lower_high_price) or current_price >= lower_high_price:
            logging.info("%s skipped: lower_high_already_reclaimed", self.ALGO)
            return
        # Use the latest confirmed swing peak for the stop and half its entry-distance percentage for trailing deviation.
        trailing_deviation_pct = float(
            (
                (Decimal(str(lower_high_price)) / Decimal(str(current_price)) - 1) * 50
            ).quantize(Decimal("0.0001"), rounding=ROUND_CEILING)
        )
        # Round upward to keep the short stop outside the pattern.
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
        if not 0 < stop_loss_pct < 100:
            logging.info("%s skipped: lower_high_stop_invalid", self.ALGO)
            return

        confirmation_open_time = int(lower_high["confirmation_open_time"])
        if self._already_emitted(confirmation_open_time):
            logging.info("%s skipped: lower_high_already_emitted", self.ALGO)
            return

        top_gainer_score = (
            self.TOP_GAINER_SCORE_BONUS if top_gainer_watch is not None else 0.0
        )
        max_gain_24h_pct = (
            top_gainer_watch["top_gainer_watch_max_gain_24h_pct"]
            if top_gainer_watch is not None
            else None
        )
        strong_gainer = (
            max_gain_24h_pct is not None
            and max_gain_24h_pct >= self.STRONG_GAIN_THRESHOLD_PCT
        )
        stretch = self._stretch_context(max_gain_24h_pct, lower_high_price)
        stretch_score = (
            (self.RECORD_GAIN_SCORE_BONUS if stretch["stretch_record_gain"] else 0.0)
            + (self.NEW_HIGH_SCORE_BONUS if stretch["stretch_new_high"] else 0.0)
            + (self.EXTENSION_SCORE_BONUS if stretch["stretch_extended"] else 0.0)
            if stretch is not None
            else 0.0
        )

        score_before_chop_penalty = round_numbers(
            self.BASE_SCORE
            + top_gainer_score
            + (self.STRONG_GAIN_SCORE_BONUS if strong_gainer else 0.0)
            + stretch_score,
            4,
        )
        chop_score_penalty = crossings * CHOP_CROSSING_SCORE_PENALTY
        score = round_numbers(score_before_chop_penalty - chop_score_penalty, 4)
        futures_leverage = self.futures_leverage_for_score(score)

        fiat_order_size = round_numbers(
            self.at_consumer.autotrade_settings.base_order_size
            * self.FIAT_ORDER_SIZE_FRACTION,
            8,
        )
        quote_asset = self.current_symbol_data.quote_asset
        context = self.ti.latest_market_context
        autotrade_enabled = False if self.config.env == "development" else True
        kucoin_link, terminal_link = build_links_msg(
            self.config.frontend_domain,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        indicators: dict[str, Any] = {
            "entry_reason": "lower_high_breakdown",
            **(top_gainer_watch if top_gainer_watch is not None else {}),
            "current_top_gainer": top_gainer_watch is not None,
            "top_gainer_score_bonus": top_gainer_score,
            "strong_gainer": strong_gainer,
            "strong_gainer_threshold_pct": self.STRONG_GAIN_THRESHOLD_PCT,
            "lower_high_first_peak": lower_high["earlier_high"],
            "lower_high_second_peak": lower_high["later_high"],
            "lower_high_drop_pct": lower_high["drop_pct"],
            "lower_high_confirmation_open_time": confirmation_open_time,
            "breadth_higher_low_confirmed": breadth_context is not None,
            "btc_higher_low_confirmed": btc_context is not None,
            "price_crossings_six_hours": crossings,
            "chop_crossing_threshold": CHOP_CROSSING_THRESHOLD,
            "chop_score_penalty_per_crossing": CHOP_CROSSING_SCORE_PENALTY,
            "chop_score_penalty": chop_score_penalty,
            "score_before_chop_penalty": score_before_chop_penalty,
            "futures_leverage": futures_leverage,
            "futures_leverage_score_min": round_numbers(self.MIN_SCORE, 4),
            "futures_leverage_score_max": self.MAX_SCORE,
            "chop_lookback_bars": (
                CHOP_LOOKBACK_HOURS
                * 60
                * 60
                * 1000
                // self.candlestick_interval.get_ms()
            ),
            "lower_high_stop_buffer_pct": self.LOWER_HIGH_STOP_BUFFER_PCT,
            "stop_loss_source": "lower_high",
            "stop_loss_price_at_signal": stop_loss_price,
            "stop_loss_pct": stop_loss_pct,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": trailing_deviation_pct,
            "protective_exit": "dynamic_position_sizing",
            **(breadth_context if breadth_context is not None else {}),
            **(btc_context if btc_context is not None else {}),
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
                dynamic_position_sizing=True,
                fiat_order_size=fiat_order_size,
                futures_leverage=futures_leverage,
                stop_loss=stop_loss_pct,
                take_profit=self.TRAILING_PROFIT_PCT,
                trailing=True,
                trailing_deviation=trailing_deviation_pct,
                trailing_profit=self.TRAILING_PROFIT_PCT,
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
        # Preserve the original stop-risk budget after open-interest sizing.
        reference_stop_price = (
            Decimal(str(lower_high_price))
            * (1 + Decimal(str(self.RISK_BUDGET_STOP_BUFFER_PCT)) / 100)
        ).quantize(Decimal(1).scaleb(-self.price_precision), rounding=ROUND_CEILING)
        reference_stop_pct = (
            (reference_stop_price / Decimal(str(current_price)) - 1) * 100
        ).quantize(Decimal("0.0001"), rounding=ROUND_CEILING)
        margin_before_risk_sizing = value.bot_params.fiat_order_size
        margin_multiplier = min(
            Decimal(1), reference_stop_pct / Decimal(str(stop_loss_pct))
        )
        fiat_order_size = float(
            (Decimal(str(margin_before_risk_sizing)) * margin_multiplier).quantize(
                Decimal("0.00000001"), rounding=ROUND_DOWN
            )
        )
        value.bot_params.fiat_order_size = fiat_order_size
        indicators["stop_risk_sizing"] = {
            "margin_before": margin_before_risk_sizing,
            "margin_after": fiat_order_size,
            "reference_stop_buffer_pct": self.RISK_BUDGET_STOP_BUFFER_PCT,
            "reference_stop_pct": float(reference_stop_pct),
            "margin_multiplier": float(margin_multiplier),
        }

        top_gainer_lines = (
            f"- Top-gainer rank / 24h move: {top_gainer_watch['top_gainer_rank']} / {round_numbers(top_gainer_watch['top_gainer_price_change_24h_pct'], 2)}%\n"
            f"- Continuous top-10 watch: {round_numbers(top_gainer_watch['top_gainer_watch_hours'], 2)}h; maximum 24h gain: {round_numbers(top_gainer_watch['top_gainer_watch_max_gain_24h_pct'], 2)}%"
            if top_gainer_watch is not None
            else f"- Top-gainer context: no fresh top-10 membership above {self.MIN_TOP_GAINER_GAIN_PCT}%"
        )
        breadth_line = (
            f"- Breadth higher low confirmed: {round_numbers(breadth_context['breadth_higher_low_first_trough'], 4)} -> {round_numbers(breadth_context['breadth_higher_low_second_trough'], 4)}; latest index: {round_numbers(breadth_context['breadth_latest'], 4)}"
            if breadth_context is not None
            else "- Breadth higher low confirmed: No"
        )
        btc_line = (
            f"- BTC higher low confirmed: {round_numbers(btc_context['btc_higher_low_first_trough'], 4)} -> {round_numbers(btc_context['btc_higher_low_second_trough'], 4)}; latest low: {round_numbers(btc_context['btc_latest'], 4)}"
            if btc_context is not None
            else "- BTC higher low confirmed: No"
        )
        msg = f"""
            - [{self.config.env}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: SHORT ENTRY
            - Current price: {round_numbers(current_price, self.price_precision)}
            - Rule intent: SHORT when price confirms a lower high and BTC or the breadth index has a confirmed higher low; current top-10 gainers above {self.MIN_TOP_GAINER_GAIN_PCT}% receive a score bonus
            {top_gainer_lines}
            - Top-gainer score bonus: +{top_gainer_score}
            - Strong-gainer threshold (>= {self.STRONG_GAIN_THRESHOLD_PCT}%): {"Yes" if strong_gainer else "No"}
            - Lower high first / second peak: {round_numbers(lower_high["earlier_high"], self.price_precision)} / {round_numbers(lower_high["later_high"], self.price_precision)}
            {breadth_line}
            {btc_line}
            {self._stretch_telegram_line(stretch, stretch_score, max_gain_24h_pct)}
            {format_context_timestamp_line(context)}
            {self.ti.regime_telegram_lines()}
            - Initial margin: {fiat_order_size} {quote_asset}
            - Initial risk sizing: margin reduced from {margin_before_risk_sizing} {quote_asset} using the prior {self.RISK_BUDGET_STOP_BUFFER_PCT}% buffer's stop-risk budget; subsequent adjustments change exposure
            - Current-price crossings over 6h: {crossings}; rejected at {CHOP_CROSSING_THRESHOLD}+; score penalty: {CHOP_CROSSING_SCORE_PENALTY} per crossing
            - Initial reduction threshold: {self.LOWER_HIGH_STOP_BUFFER_PCT}% above lower high at {stop_loss_price} ({stop_loss_pct}%)
            - Position sizing: default 25% reduction at stop threshold / increase at {self.TRAILING_PROFIT_PCT}% profit; repeats with thresholds reset after fills
            - Stop behavior: dynamic sizing replaces full stop; no reversal position
            - Trailing fallback: arms after {self.TRAILING_PROFIT_PCT}% profit with {trailing_deviation_pct}% deviation
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Confidence score: {score}
            - Futures leverage: {futures_leverage}x (score range {round_numbers(self.MIN_SCORE, 4)}–{self.MAX_SCORE})
            - Autotrade: {"enabled" if autotrade_enabled else "disabled; notification only"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        try:
            await self.ti.dispatch_signal_record(value=value, indicators=indicators)
            await self.telegram_consumer.dispatch_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)
        finally:
            # Prevent duplicate signals if persistence succeeds but dispatch fails.
            self._mark_emitted(confirmation_open_time)

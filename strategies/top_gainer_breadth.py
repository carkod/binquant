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
    RecoveryParams,
    SignalsConsumer,
    breadth_momentum_reversal,
    btc_trend_confirms,
    round_numbers,
    timestamp_sort_key,
)

from shared.utils import build_links_msg, format_context_timestamp_line
from strategies.lower_high_pattern import LowerHighPattern

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopGainerBreadth:
    """Short a current 2nd-to-11th ranked gainer as bullish momentum fails.

    Entry requires one coherent bearish setup:
    - market-breadth's fast EMA(3) crosses below its slower breadth average
      while breadth remains extended bullish (>= 0.15);
    - BTC's 15m close is below its EMA(20);
    - the symbol's 15m candles have just confirmed a lower high.

    The resulting futures short uses a stop at the upper Bollinger Band,
    capped at 4%, and dynamic trailing protection. A confirmed adverse
    breakout through that stop closes the short and opens a recovery long.
    Once open, binbot streaming owns the position lifecycle.
    """

    ALGO = "top_gainer_breadth"

    TOP_GAINER_RANK_START = 2
    TOP_GAINER_RANK_END = 11
    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    ENTRY_COOLDOWN_MINUTES = 60
    TRAILING_PROFIT_PCT = 3.5
    TRAILING_DEVIATION_PCT = 2.5
    MAX_STOP_LOSS_PCT = 4.0

    MIN_BREADTH_HISTORY = 12
    BREADTH_FAST_EMA_SPAN = 3
    BREADTH_EXTENSION_THRESHOLD = 0.15
    BREADTH_CEILING = 0.6
    MAX_BREADTH_AGE_SECONDS = 30 * 60
    MAX_GAINERS_SNAPSHOT_AGE_SECONDS = 75 * 60

    MIN_BTC_HISTORY = 20
    BTC_TREND_EMA_SPAN = 20

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
        self._last_emitted_breadth_timestamp: int | None = None

    def _top_gainer_entry(self) -> tuple[int, float] | None:
        if not self.gainers_losers_series:
            return None

        latest_snapshot = self.gainers_losers_series[0]
        if not self._timestamp_is_fresh(
            latest_snapshot.recorded_at,
            self.MAX_GAINERS_SNAPSHOT_AGE_SECONDS,
        ):
            return None
        return next(
            (
                (rank, entry.price_change_percent)
                for rank, entry in enumerate(
                    latest_snapshot.top_gainers[
                        self.TOP_GAINER_RANK_START - 1 : self.TOP_GAINER_RANK_END
                    ],
                    start=self.TOP_GAINER_RANK_START,
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
    def _stop_loss_pct(cls, current_price: float, bb_high: float) -> float | None:
        if (
            not isfinite(current_price)
            or not isfinite(bb_high)
            or current_price <= 0
            or bb_high <= current_price
        ):
            return None

        stop_loss = ((bb_high / current_price) - 1) * 100
        return round_numbers(min(stop_loss, cls.MAX_STOP_LOSS_PCT), 4)

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

    def _already_emitted(self, breadth_timestamp: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_breadth_timestamp == breadth_timestamp
        return (
            self.strategy_cooldowns.get((self.ALGO, self.symbol)) == breadth_timestamp
        )

    def _mark_emitted(self, breadth_timestamp: int) -> None:
        self._last_emitted_breadth_timestamp = breadth_timestamp
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = breadth_timestamp

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            return

        top_gainer_entry = self._top_gainer_entry()
        if top_gainer_entry is None:
            logging.info("%s skipped: symbol_not_in_ranked_gainer_window", self.ALGO)
            return
        top_gainer_rank, price_change_24h = top_gainer_entry

        breadth_values, breadth_reason = breadth_momentum_reversal(
            self.market_breadth_data,
            direction=-1,
            min_history=self.MIN_BREADTH_HISTORY,
            fast_ema_span=self.BREADTH_FAST_EMA_SPAN,
            extension_threshold=self.BREADTH_EXTENSION_THRESHOLD,
        )
        if breadth_values is None:
            logging.info("%s skipped: %s", self.ALGO, breadth_reason)
            return
        if not self._timestamp_is_fresh(
            breadth_values["breadth_timestamp"],
            self.MAX_BREADTH_AGE_SECONDS,
        ):
            logging.info("%s skipped: stale_market_breadth", self.ALGO)
            return

        btc_trend = btc_trend_confirms(
            self.ti.df_btc_15m,
            direction=-1,
            min_history=self.MIN_BTC_HISTORY,
            trend_ema_span=self.BTC_TREND_EMA_SPAN,
        )
        if btc_trend is None:
            logging.info("%s skipped: btc_not_in_downtrend", self.ALGO)
            return
        btc_close, btc_trend_ema = btc_trend

        lower_high = self._fresh_lower_high()
        if lower_high is None:
            logging.info("%s skipped: no_fresh_confirmed_lower_high", self.ALGO)
            return

        stop_loss = self._stop_loss_pct(current_price, bb_high)
        if stop_loss is None:
            logging.info("%s skipped: upper_bollinger_stop_invalid", self.ALGO)
            return
        stop_loss_price = round_numbers(
            current_price + (current_price * stop_loss / 100), self.price_precision
        )
        stop_loss_source = (
            "max_stop_loss_cap"
            if stop_loss == self.MAX_STOP_LOSS_PCT and bb_high > stop_loss_price
            else "upper_bollinger_band"
        )

        breadth_timestamp = int(breadth_values["breadth_timestamp"] * 1000)
        if self._already_emitted(breadth_timestamp):
            logging.info("%s skipped: breadth_cross_already_emitted", self.ALGO)
            return
        self._mark_emitted(breadth_timestamp)

        high_conviction_ceiling_reached = (
            breadth_values["market_breadth"] >= self.BREADTH_CEILING
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
            **breadth_values,
            "entry_reason": breadth_reason,
            "breadth_ceiling": self.BREADTH_CEILING,
            "high_conviction_ceiling_reached": high_conviction_ceiling_reached,
            "top_gainer_rank": top_gainer_rank,
            "top_gainer_price_change_24h_pct": price_change_24h,
            "btc_close_15m": btc_close,
            "btc_trend_ema": btc_trend_ema,
            "lower_high_first_peak": lower_high["earlier_high"],
            "lower_high_second_peak": lower_high["later_high"],
            "lower_high_drop_pct": lower_high["drop_pct"],
            "lower_high_confirmation_open_time": lower_high["confirmation_open_time"],
            "stop_loss_source": stop_loss_source,
            "stop_loss_price_at_signal": stop_loss_price,
            "stop_loss_pct": stop_loss,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
            "protective_exit": "bot_managed_stop_with_recovery_long",
        }

        value = SignalsConsumer(
            direction=Position.short.value.upper(),
            autotrade=True,
            current_price=float(current_price),
            score=1.0,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.short,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=True,
                fiat_order_size=fiat_order_size,
                stop_loss=stop_loss,
                trailing=True,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                trailing_profit=self.TRAILING_PROFIT_PCT,
                margin_short_reversal=True,
                recovery_params=RecoveryParams(),
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
            - Rule intent: SHORT a current 24h gainer ranked 2nd-11th when breadth momentum turns bearish, BTC trends down, and price confirms a lower high
            - Top-gainer rank / 24h move: {top_gainer_rank} / {round_numbers(price_change_24h, 2)}%
            - Market breadth (extended bullish) at signal: {round_numbers(breadth_values["market_breadth"], 4)}
            - High-conviction ceiling (>= {self.BREADTH_CEILING}) reached: {"Yes" if high_conviction_ceiling_reached else "No"}
            - Breadth momentum oscillator previous / current: {round_numbers(breadth_values["previous_breadth_oscillator"], 4)} / {round_numbers(breadth_values["breadth_oscillator"], 4)}
            - BTC 15m close / EMA{self.BTC_TREND_EMA_SPAN}: {round_numbers(btc_close, self.price_precision)} / {round_numbers(btc_trend_ema, self.price_precision)}
            - Lower high first / second peak: {round_numbers(lower_high["earlier_high"], self.price_precision)} / {round_numbers(lower_high["later_high"], self.price_precision)}
            {format_context_timestamp_line(context)}
            - Max margin: {fiat_order_size} {quote_asset}
            - Stop loss: {stop_loss_source} at {stop_loss_price} ({stop_loss}%)
            - Stop behavior: confirmed adverse breakout closes the SHORT reduce-only and opens one recovery LONG
            - Trailing profit / deviation: {self.TRAILING_PROFIT_PCT}% / {self.TRAILING_DEVIATION_PCT}%
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        await self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

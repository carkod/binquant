import logging
from math import isfinite
from typing import TYPE_CHECKING

from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketBreadthSeries,
    MarketType,
    Position,
    SignalsConsumer,
    breadth_momentum_reversal,
    btc_trend_confirms,
    round_numbers,
)

from shared.strategy_mixin import StrategyMixin
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class TopGainerBreadth(StrategyMixin):
    """Long a current 2nd-to-11th ranked gainer when market-breadth momentum
    reverses off an extended bearish reading, confirmed by BTC trending up.
    Closes that bot again once the mirrored bearish setup shows up.

    Entry requires all three:
    - a fast/slow EMA crossover of market_breadth turning bullish (the slow
      EMA is the API-provided market_breadth_ma; the fast EMA is computed
      here), i.e. breadth momentum turning up;
    - market_breadth itself still extended bearish (<= -0.15) at that bar,
      i.e. the broad market is still net-negative when momentum turns;
    - BTC's 15m close above its own EMA(20), i.e. BTC already trending up.

    A reversal bar that reaches BREADTH_FLOOR (-0.6) or deeper is tagged in
    the indicators/message as high_conviction_floor_reached. -0.6 was picked
    as a round number above the ~-0.66 to -0.77 weekly lows observed across
    the last 30 days of production market_breadth history: a print that
    deep already satisfies the -0.15 entry condition above, so this tag does
    not change whether the bot opens. It exists purely so entries at the
    market's genuine structural floor can be told apart from the far more
    common, softer -0.15 crossings when reviewing signal performance.

    Exit mirrors it exactly (momentum turning bearish, breadth still
    extended bullish >= 0.15, BTC below its EMA(20)) and deactivates any
    active bot for this algo/symbol via StrategyMixin.deactivate_active_bot.
    Every entry also has an exchange-native stop capped at 4%, plus dynamic
    trailing profit protection. The stop closes the long rather than opening
    a reversal position, so it remains effective if streaming is unavailable.

    The reversal math itself (pybinbot.breadth_momentum_reversal /
    pybinbot.btc_trend_confirms) is shared with binbot/streaming's
    TopGainerBreadthLifecycleStrategy, which checks the same exit condition
    on every live price tick as a faster-reacting mirror of this one. Keeping
    the math in pybinbot means both surfaces agree on the same signal
    instead of maintaining two copies that could drift out of calibration.

    This combination was backtested against 52 days of market-breadth, BTC
    price, and BTC funding-rate history: alone it does not reliably predict a
    full breadth reversal (most crossings are noise), but used as an
    entry/exit pair for long-only trades it produced a positive-expectancy
    sample (6 of 12 simulated trades profitable, average win outsizing
    average loss ~2.7x).
    """

    ALGO = "top_gainer_breadth"
    EXIT_ALGO = "top_gainer_breadth_exit"

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
    # Informational only: tags a reversal bar that reaches this deep as
    # high-conviction. It does not gate entry (see class docstring).
    BREADTH_FLOOR = -0.6

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
        self.binbot_api = cls.binbot_api
        self.market_breadth_data: MarketBreadthSeries | None = cls.market_breadth_data
        self.gainers_losers_series = cls.gainers_losers_series
        self.strategy_cooldowns = cls.strategy_cooldowns
        self._last_emitted_breadth_timestamp: int | None = None
        self._last_exited_breadth_timestamp: int | None = None

    def _top_gainer_entry(self) -> tuple[int, float] | None:
        if not self.gainers_losers_series:
            return None

        latest_snapshot = self.gainers_losers_series[0]
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

    def _already_exited(self, breadth_timestamp: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_exited_breadth_timestamp == breadth_timestamp
        return (
            self.strategy_cooldowns.get((self.EXIT_ALGO, self.symbol))
            == breadth_timestamp
        )

    def _mark_exited(self, breadth_timestamp: int) -> None:
        self._last_exited_breadth_timestamp = breadth_timestamp
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.EXIT_ALGO, self.symbol)] = breadth_timestamp

    def _process_exit(self) -> None:
        exit_values, exit_reason = breadth_momentum_reversal(
            self.market_breadth_data,
            direction=-1,
            min_history=self.MIN_BREADTH_HISTORY,
            fast_ema_span=self.BREADTH_FAST_EMA_SPAN,
            extension_threshold=self.BREADTH_EXTENSION_THRESHOLD,
        )
        if exit_values is None:
            return

        btc_trend = btc_trend_confirms(
            self.ti.df_btc_15m,
            direction=-1,
            min_history=self.MIN_BTC_HISTORY,
            trend_ema_span=self.BTC_TREND_EMA_SPAN,
        )
        if btc_trend is None:
            logging.info("%s skipped: btc_not_in_downtrend", self.EXIT_ALGO)
            return
        btc_close, btc_trend_ema = btc_trend

        exit_timestamp = int(exit_values["breadth_timestamp"] * 1000)
        if self._already_exited(exit_timestamp):
            return

        active_bots = self.binbot_api.get_bots_by_name(self.ALGO, self.symbol)
        if not active_bots:
            self._mark_exited(exit_timestamp)
            logging.info("%s skipped: no_active_bot_for_symbol", self.EXIT_ALGO)
            return

        closed_bots = 0
        for bot in active_bots:
            succeeded, result = self.deactivate_active_bot(
                bot_id=str(bot.id),
                symbol=self.symbol,
                source_label=self.ALGO,
            )
            logging.info("%s (%s): %s", self.EXIT_ALGO, exit_reason, result)
            closed_bots += int(succeeded)

        if closed_bots != len(active_bots):
            logging.warning(
                "%s will retry: closed %s of %s active bots",
                self.EXIT_ALGO,
                closed_bots,
                len(active_bots),
            )
            return

        self._mark_exited(exit_timestamp)

        msg = f"""
            - [{self.config.env}] <strong>#{self.EXIT_ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG EXIT
            - Rule intent: close the LONG once breadth momentum reverses off an extended bullish reading and BTC confirms a downtrend
            - Market breadth (extended bullish) at signal: {round_numbers(exit_values["market_breadth"], 4)}
            - Breadth momentum oscillator previous / current: {round_numbers(exit_values["previous_breadth_oscillator"], 4)} / {round_numbers(exit_values["breadth_oscillator"], 4)}
            - BTC 15m close / EMA{self.BTC_TREND_EMA_SPAN}: {round_numbers(btc_close, self.price_precision)} / {round_numbers(btc_trend_ema, self.price_precision)}
            - Bots closed: {closed_bots}
        """
        self.telegram_consumer.dispatch_signal(msg)

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            return

        self._process_exit()

        top_gainer_entry = self._top_gainer_entry()
        if top_gainer_entry is None:
            logging.info("%s skipped: symbol_not_in_ranked_gainer_window", self.ALGO)
            return
        top_gainer_rank, price_change_24h = top_gainer_entry

        breadth_values, breadth_reason = breadth_momentum_reversal(
            self.market_breadth_data,
            direction=1,
            min_history=self.MIN_BREADTH_HISTORY,
            fast_ema_span=self.BREADTH_FAST_EMA_SPAN,
            extension_threshold=self.BREADTH_EXTENSION_THRESHOLD,
        )
        if breadth_values is None:
            logging.info("%s skipped: %s", self.ALGO, breadth_reason)
            return

        btc_trend = btc_trend_confirms(
            self.ti.df_btc_15m,
            direction=1,
            min_history=self.MIN_BTC_HISTORY,
            trend_ema_span=self.BTC_TREND_EMA_SPAN,
        )
        if btc_trend is None:
            logging.info("%s skipped: btc_not_in_uptrend", self.ALGO)
            return
        btc_close, btc_trend_ema = btc_trend

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

        breadth_timestamp = int(breadth_values["breadth_timestamp"] * 1000)
        if self._already_emitted(breadth_timestamp):
            logging.info("%s skipped: breadth_cross_already_emitted", self.ALGO)
            return
        self._mark_emitted(breadth_timestamp)

        high_conviction_floor_reached = (
            breadth_values["market_breadth"] <= self.BREADTH_FLOOR
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
            "breadth_floor": self.BREADTH_FLOOR,
            "high_conviction_floor_reached": high_conviction_floor_reached,
            "top_gainer_rank": top_gainer_rank,
            "top_gainer_price_change_24h_pct": price_change_24h,
            "btc_close_15m": btc_close,
            "btc_trend_ema": btc_trend_ema,
            "stop_loss_source": stop_loss_source,
            "stop_loss_price_at_signal": stop_loss_price,
            "stop_loss_pct": stop_loss,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
            "protective_exit": "exchange_native_reduce_only_stop",
        }

        value = SignalsConsumer(
            direction=Position.long.value.upper(),
            autotrade=True,
            current_price=float(current_price),
            score=1.0,
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

        msg = f"""
            - [{self.config.env}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(current_price, self.price_precision)}
            - Rule intent: LONG a current 24h gainer ranked 2nd-11th when breadth momentum reverses off an extended bearish reading and BTC confirms an uptrend
            - Top-gainer rank / 24h move: {top_gainer_rank} / {round_numbers(price_change_24h, 2)}%
            - Market breadth (extended bearish) at signal: {round_numbers(breadth_values["market_breadth"], 4)}
            - High-conviction floor (<= {self.BREADTH_FLOOR}) reached: {"Yes" if high_conviction_floor_reached else "No"}
            - Breadth momentum oscillator previous / current: {round_numbers(breadth_values["previous_breadth_oscillator"], 4)} / {round_numbers(breadth_values["breadth_oscillator"], 4)}
            - BTC 15m close / EMA{self.BTC_TREND_EMA_SPAN}: {round_numbers(btc_close, self.price_precision)} / {round_numbers(btc_trend_ema, self.price_precision)}
            {format_context_timestamp_line(context)}
            - Max margin: {fiat_order_size} {quote_asset}
            - Stop loss: {stop_loss_source} at {stop_loss_price} ({stop_loss}%)
            - Stop behavior: exchange-native reduce-only close; no reversal position
            - Trailing profit / deviation: {self.TRAILING_PROFIT_PCT}% / {self.TRAILING_DEVIATION_PCT}%
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        await self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from os import getenv
from typing import TYPE_CHECKING, Any, cast

from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from pandas import DataFrame

    from producers.context_evaluator import ContextEvaluator


@dataclass(frozen=True)
class GradualGainerCandidate:
    candle_open_time: int
    symbol: str
    rank_score: float
    dispatch: Callable[[], Awaitable[None]]


class GradualGainerPortfolioSelector:
    """Dispatch only the strongest gradual-gainer reclaim in each UTC hour."""

    def __init__(self) -> None:
        self._latest_hour: int | None = None
        self._candidates: dict[int, dict[str, GradualGainerCandidate]] = {}

    async def submit(self, candidate: GradualGainerCandidate) -> bool:
        hour = candidate.candle_open_time // 3_600_000
        if self._latest_hour is None or hour > self._latest_hour:
            for completed_hour in sorted(self._candidates):
                if completed_hour >= hour:
                    break
                await self._dispatch_winner(completed_hour)
            self._latest_hour = hour
        if self._latest_hour is not None and hour < self._latest_hour:
            return False

        cohort = self._candidates.setdefault(hour, {})
        current = cohort.get(candidate.symbol)
        if current is None or candidate.rank_score >= current.rank_score:
            cohort[candidate.symbol] = candidate
        return True

    async def flush(self) -> None:
        for hour in sorted(self._candidates):
            await self._dispatch_winner(hour)

    async def _dispatch_winner(self, hour: int) -> None:
        cohort = self._candidates.pop(hour, {})
        if not cohort:
            return
        winner = max(cohort.values(), key=lambda item: (item.rank_score, item.symbol))
        try:
            await winner.dispatch()
        except Exception:
            logging.exception(
                "Gradual-gainer portfolio winner failed for %s.", winner.symbol
            )


class GradualGainerRetest:
    """Buy a completed-candle reclaim from a sustained relative-strength leader.

    Once the reclaim closes, binbot places a discounted retest limit for one
    configured candle. This avoids buying an intrabar wick or chasing the
    confirmation candle at its live price.
    """

    ALGO = "gradual_gainer_retest"
    MIN_HISTORY = 100
    WATCHLIST_HOURS = 8
    RS_QUANTILE = 0.80
    RS_LOOKBACK = 96
    BREAKOUT_LOOKBACK = 8
    EMA_WINDOW = 20
    SUPPORT_TOLERANCE_PCT = 0.5
    MAX_PULLBACK_PCT = 0.04
    MAX_MARKET_STRESS_SCORE = 0.25
    MAX_SYMBOL_ATR_PCT = 0.06
    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    STOP_LOSS_PCT = 2.0
    TRAILING_PROFIT_PCT = 3.0
    TRAILING_DEVIATION_PCT = 1.5
    RETEST_DISCOUNT_PCT = 0.5
    RETEST_WAIT_BARS = 1
    ENTRY_COOLDOWN_MINUTES = 240

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
        self.strategy_cooldowns = cls.strategy_cooldowns
        self.strategy_states = getattr(cls, "strategy_states", None)
        self.portfolio_selector: GradualGainerPortfolioSelector | None = getattr(
            cls, "gradual_gainer_portfolio_selector", None
        )

    @property
    def _state_key(self) -> tuple[str, str]:
        return self.ALGO, self.symbol

    def _state(self) -> dict[str, float | int] | None:
        if self.strategy_states is None:
            return None
        return self.strategy_states.get(self._state_key)

    def _set_watchlist(self, *, candle_open_time: int, breakout_level: float) -> None:
        if self.strategy_states is not None:
            self.strategy_states[self._state_key] = {
                "started_at": candle_open_time,
                "expires_at": candle_open_time + self.WATCHLIST_HOURS * 3_600_000,
                "breakout_level": breakout_level,
            }

    def _clear_watchlist(self) -> None:
        if self.strategy_states is not None:
            self.strategy_states.pop(self._state_key, None)

    @staticmethod
    def _completed_candles(df: "DataFrame", now_ms: int) -> "DataFrame":
        if "close_time" not in df.columns:
            return df.iloc[0:0]
        return df.loc[df["close_time"] < now_ms]

    @staticmethod
    def _relative_strengths(
        df: "DataFrame", btc_df: "DataFrame"
    ) -> tuple[float, float] | None:
        if len(df) < 25 or len(btc_df) < 25:
            return None
        if not {"open_time", "close"}.issubset(btc_df.columns):
            return None
        btc_by_time = {
            int(cast(Any, row.open_time)): float(cast(Any, row.close))
            for row in btc_df.itertuples()
        }
        times = [int(value) for value in df["open_time"].iloc[-25:]]
        if any(time not in btc_by_time for time in times):
            return None
        closes = df["close"].iloc[-25:].astype(float).tolist()
        btc_closes = [btc_by_time[time] for time in times]
        if min(*closes, *btc_closes) <= 0:
            return None
        return (
            closes[-1] / closes[-9] - btc_closes[-1] / btc_closes[-9],
            closes[-1] / closes[-25] - btc_closes[-1] / btc_closes[-25],
        )

    @classmethod
    def _leadership_allows(
        cls, df: "DataFrame", btc_df: "DataFrame"
    ) -> tuple[bool, float, float]:
        strengths = cls._relative_strengths(df, btc_df)
        if strengths is None or len(df) < cls.MIN_HISTORY:
            return False, 0.0, 0.0
        rs_2h, rs_6h = strengths
        close = df["close"].astype(float)
        btc_by_time = {
            int(cast(Any, row.open_time)): float(cast(Any, row.close))
            for row in btc_df.itertuples()
        }
        rs_2h_history: list[float] = []
        rs_6h_history: list[float] = []
        for index in range(max(24, len(df) - cls.RS_LOOKBACK), len(df)):
            times = [int(df["open_time"].iloc[index - offset]) for offset in (0, 8, 24)]
            if any(time not in btc_by_time for time in times):
                continue
            asset_now, asset_2h, asset_6h = (
                float(close.iloc[index - offset]) for offset in (0, 8, 24)
            )
            btc_now, btc_2h, btc_6h = (btc_by_time[time] for time in times)
            if min(asset_now, asset_2h, asset_6h, btc_now, btc_2h, btc_6h) > 0:
                rs_2h_history.append(asset_now / asset_2h - btc_now / btc_2h)
                rs_6h_history.append(asset_now / asset_6h - btc_now / btc_6h)
        if len(rs_2h_history) < 20 or len(rs_6h_history) < 20:
            return False, rs_2h, rs_6h
        rs_2h_threshold = sorted(rs_2h_history)[
            int((len(rs_2h_history) - 1) * cls.RS_QUANTILE)
        ]
        rs_6h_threshold = sorted(rs_6h_history)[
            int((len(rs_6h_history) - 1) * cls.RS_QUANTILE)
        ]
        return (
            rs_2h > 0
            and rs_6h > 0
            and rs_2h >= rs_2h_threshold
            and rs_6h >= rs_6h_threshold,
            rs_2h,
            rs_6h,
        )

    def _risk_allows(self) -> tuple[bool, str]:
        context = self.ti.latest_market_context
        features = resolve_symbol_features(context=context, symbol=self.symbol)
        if context is None:
            return False, "market_context_unavailable"
        if context.market_stress_score >= self.MAX_MARKET_STRESS_SCORE:
            return False, "market_stress_too_high"
        if context.btc_regime_score < -0.2 and context.btc_return < 0:
            return False, "btc_context_down"
        if features is None:
            return False, "symbol_regime_unavailable"
        if features.atr_pct > self.MAX_SYMBOL_ATR_PCT:
            return False, "symbol_atr_too_high"
        if features.micro_regime_transition in {"BREAKDOWN", "ENTERED_TREND_DOWN"}:
            return False, "symbol_transition_not_long"
        if features.micro_regime == "TREND_DOWN" and features.trend_score < 0:
            return False, "symbol_trend_down"
        return True, "risk_profile_allows_long"

    def _fiat_order_size(self) -> float:
        settings = getattr(self.at_consumer, "autotrade_settings", None)
        base_order_size = float(getattr(settings, "base_order_size", 0.0) or 0.0)
        return round_numbers(base_order_size * self.FIAT_ORDER_SIZE_FRACTION, 8)

    async def signal(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            return
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        df = self._completed_candles(self.ti.df_15m, now_ms=now_ms)
        btc_df = self._completed_candles(self.ti.df_btc_15m, now_ms=now_ms)
        if len(df) < self.MIN_HISTORY or not {
            "open_time",
            "open",
            "high",
            "low",
            "close",
        }.issubset(df.columns):
            return
        candidate = df.iloc[-1]
        candle_open_time = int(candidate["open_time"])
        state = self._state()
        if state is not None and candle_open_time > int(state["expires_at"]):
            self._clear_watchlist()
            state = None

        leader, rs_2h, rs_6h = self._leadership_allows(df, btc_df)
        if state is None and leader:
            self._set_watchlist(
                candle_open_time=candle_open_time,
                breakout_level=float(
                    df["high"].iloc[-self.BREAKOUT_LOOKBACK - 1 : -1].max()
                ),
            )
            return
        if state is None:
            return

        previous = df.iloc[-2]
        breakout_level = float(state["breakout_level"])
        ema20 = float(
            df["close"].ewm(span=self.EMA_WINDOW, adjust=False).mean().iloc[-2]
        )
        support_held = float(previous["low"]) >= breakout_level * (
            1 - self.SUPPORT_TOLERANCE_PCT / 100
        ) or float(previous["low"]) >= ema20 * (1 - self.SUPPORT_TOLERANCE_PCT / 100)
        shallow_pullback = float(previous["close"]) >= breakout_level * (
            1 - self.MAX_PULLBACK_PCT
        )
        green_reclaim = float(candidate["close"]) > float(candidate["open"]) and float(
            candidate["close"]
        ) > float(previous["high"])
        if not support_held or not shallow_pullback or not green_reclaim:
            return
        risk_allowed, risk_reason = self._risk_allows()
        if not risk_allowed:
            logging.info("%s skipped for %s: %s", self.ALGO, self.symbol, risk_reason)
            return
        if (
            self.strategy_cooldowns is not None
            and self.strategy_cooldowns.get((self.ALGO, self.symbol))
            == candle_open_time
        ):
            return
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = candle_open_time

        activity_warning = self._activity_warning()
        price_tracker_warning = self._price_tracker_warning()
        rank_score = round_numbers(
            1
            + rs_2h * 4
            + rs_6h * 2
            + 0.1 * activity_warning
            + 0.1 * price_tracker_warning,
            4,
        )
        await self._submit_candidate(
            candle_open_time=candle_open_time,
            current_price=current_price,
            bb_high=bb_high,
            bb_mid=bb_mid,
            bb_low=bb_low,
            breakout_level=breakout_level,
            ema20=ema20,
            rs_2h=rs_2h,
            rs_6h=rs_6h,
            activity_warning=activity_warning,
            price_tracker_warning=price_tracker_warning,
            rank_score=rank_score,
        )
        self._clear_watchlist()

    def _activity_warning(self) -> int:
        df_5m = getattr(self.ti, "df_5m", None)
        if df_5m is None or len(df_5m) < 21:
            return 0
        from strategies.activity_burst_pump import ActivityBurstPump

        enriched = ActivityBurstPump(self.ti).compute_indicators(df_5m)
        return int(bool(enriched["qualified_signal"].tail(12).any()))

    def _price_tracker_warning(self) -> int:
        df_5m = getattr(self.ti, "df_5m", None)
        if (
            df_5m is None
            or len(df_5m) < 1
            or not {"rsi", "macd"}.issubset(df_5m.columns)
        ):
            return 0
        return int(
            float(df_5m["rsi"].iloc[-1]) < 35 and float(df_5m["macd"].iloc[-1]) < 0
        )

    async def _submit_candidate(self, **values: Any) -> None:
        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        autotrade = getenv("ENV") == "staging"
        fiat_order_size = self._fiat_order_size()
        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )
        indicators = {
            **values,
            "entry_reason": "watchlisted_shallow_retest_green_reclaim",
            "retest_discount_pct": self.RETEST_DISCOUNT_PCT,
            "retest_wait_bars": self.RETEST_WAIT_BARS,
            "route_reason": "staging_gradual_gainer_retest"
            if autotrade
            else "staging_only_shadow",
            "risk_reason": "risk_profile_allows_long",
        }
        value = SignalsConsumer(
            direction=Position.long.value.upper(),
            autotrade=autotrade,
            current_price=float(values["current_price"]),
            score=float(values["rank_score"]),
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.long,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=True,
                fiat_order_size=fiat_order_size,
                stop_loss=self.STOP_LOSS_PCT,
                trailing=True,
                trailing_profit=self.TRAILING_PROFIT_PCT,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                margin_short_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=values["bb_high"],
                bb_mid=values["bb_mid"],
                bb_low=values["bb_low"],
            ),
        )
        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG RETEST ENTRY
            - Rule intent: BUY the strongest hourly gradual-gainer candidate after a shallow breakout/EMA20 retest and green 15m reclaim
            - Portfolio rank score: {values["rank_score"]}
            - 2h / 6h relative strength vs BTC: {round_numbers(values["rs_2h"] * 100, 2)}% / {round_numbers(values["rs_6h"] * 100, 2)}%
            - Breakout level / EMA20: {round_numbers(values["breakout_level"], self.price_precision)} / {round_numbers(values["ema20"], self.price_precision)}
            - Entry limit: {self.RETEST_DISCOUNT_PCT}% below the completed reclaim close, valid for {self.RETEST_WAIT_BARS} candle
            - Activity burst / price-tracker early warning: {"Yes" if values["activity_warning"] else "No"} / {"Yes" if values["price_tracker_warning"] else "No"}
            - Market regime: {context.market_regime if context and context.market_regime else "UNAVAILABLE"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features else "UNAVAILABLE"}
            - Max margin: {fiat_order_size} {self.current_symbol_data.quote_asset}
            - Stop loss / trailing profit / deviation: {self.STOP_LOSS_PCT}% / {self.TRAILING_PROFIT_PCT}% / {self.TRAILING_DEVIATION_PCT}%
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        async def dispatch() -> None:
            self.ti.dispatch_signal_record(value=value, indicators=indicators)
            self.telegram_consumer.dispatch_signal(msg)
            await self.at_consumer.process_autotrade_restrictions(value)

        candidate = GradualGainerCandidate(
            candle_open_time=int(values["candle_open_time"]),
            symbol=self.symbol,
            rank_score=float(values["rank_score"]),
            dispatch=dispatch,
        )
        if self.portfolio_selector is None:
            await dispatch()
        else:
            await self.portfolio_selector.submit(candidate)

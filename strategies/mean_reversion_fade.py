import logging
from datetime import UTC, datetime
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING

from pandas import Series
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketType,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class MeanReversionFade:
    """
    Short-only mean-reversion fade of RSI + Bollinger Band extremes.

    Entry:
        - RSI(14) >= RSI_SHORT_MIN and hooks down from the prior candle
        - bearish confirmation candle closes at or above the upper Bollinger Band
        - volume >= VOLUME_RATIO_MIN * its 20-bar moving average
        - ATR(14) not in a volatility blowup (< ATR_SPIKE_MAX * its own
          20-bar average)
        - EMA20-vs-EMA50 trend score <= MAX_TREND_SCORE, avoiding fades of
          established uptrends

    Exit uses a fixed take profit and an ATR-sized emergency stop. The
    eight-candle maximum holding period remains a lifecycle responsibility;
    BotBase does not currently carry a maximum-holding field.

    RSI here is computed inline with Wilder/EWM smoothing to match exactly
    what was backtested — pybinbot's shared `Indicators.rsi` column uses a
    simple rolling mean instead, which would silently shift these thresholds.
    ATR and the Bollinger bands DO reuse the shared `df_15m` columns (their
    formulas match the backtest exactly).
    """

    ALGO = "mean_reversion_fade"
    CANDLE_INTERVAL_MS = 15 * 60 * 1000

    RSI_WINDOW = 14
    RSI_SHORT_MIN = 76.0

    VOLUME_MA_WINDOW = 20
    VOLUME_RATIO_MIN = 0.8

    ATR_WINDOW = 14
    ATR_MA_WINDOW = 20
    ATR_SPIKE_MAX = 2.2
    ATR_STOP_MULT = 2.0
    MAX_ENTRY_STOP_LOSS_PCT = 1.5
    EMA_FAST_WINDOW = 20
    EMA_SLOW_WINDOW = 50
    MAX_TREND_SCORE = 0.005
    TAKE_PROFIT_PCT = 2.4
    MAX_HOLDING_BARS = 8
    ENTRY_COOLDOWN_MINUTES = 60
    MAX_FIAT_ORDER_SIZE = 2.0
    MAX_MARKET_STRESS_SCORE = 0.25
    MAX_CONTEXT_COUNTER_TREND_GAP = 0.02
    MAX_SYMBOL_ATR_PCT = 0.03
    MIN_REJECTION_WICK_RATIO = 0.0

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
        self._last_emitted_candle: int | None = None

    @classmethod
    def _rsi(cls, closes: Series) -> Series:
        """Wilder-smoothed RSI, matching the validated backtest exactly."""
        delta = closes.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(
            alpha=1 / cls.RSI_WINDOW, min_periods=cls.RSI_WINDOW, adjust=False
        ).mean()
        avg_loss = loss.ewm(
            alpha=1 / cls.RSI_WINDOW, min_periods=cls.RSI_WINDOW, adjust=False
        ).mean()
        # 100*avg_gain/(avg_gain+avg_loss) is algebraically the same RSI as
        # 100-100/(1+avg_gain/avg_loss) when avg_loss>0, but — unlike dividing
        # by avg_loss directly — it resolves cleanly to 100 when a window has
        # no losses at all (a monotonic rally, exactly the short-entry
        # condition this strategy watches for), instead of turning the whole
        # window's RSI into NaN. Only the genuine flat case (no gains AND no
        # losses) needs an explicit neutral override; NaN from insufficient
        # warmup history is preserved either way.
        denom = avg_gain + avg_loss
        return (100 * avg_gain / denom).where(denom != 0, 50.0)

    def _resolve_entry(
        self,
        *,
        close: float,
        open_: float,
        high: float,
        low: float,
        bb_high: float,
        rsi_value: float,
        previous_rsi: float,
        volume: float,
        volume_ma: float,
        atr: float,
        atr_ma: float,
    ) -> tuple[Position | None, str]:
        if atr >= self.ATR_SPIKE_MAX * atr_ma:
            return None, "atr_volatility_spike"
        if volume < self.VOLUME_RATIO_MIN * volume_ma:
            return None, "volume_below_average"

        candle_range = high - low
        if candle_range <= 0:
            return None, "invalid_candle_range"

        upper_rejection_ratio = (high - max(open_, close)) / candle_range

        short_outside_band = (
            rsi_value >= self.RSI_SHORT_MIN
            and rsi_value < previous_rsi
            and close >= bb_high
            and close < open_
            and upper_rejection_ratio >= self.MIN_REJECTION_WICK_RATIO
        )
        if short_outside_band:
            return Position.short, "upper_band_outside_rsi_hook_red"

        return None, "no_fade_setup"

    @classmethod
    def _trend_score(cls, closes: Series) -> float:
        ema_fast = closes.ewm(span=cls.EMA_FAST_WINDOW, adjust=False).mean().iloc[-1]
        ema_slow = closes.ewm(span=cls.EMA_SLOW_WINDOW, adjust=False).mean().iloc[-1]
        if ema_slow == 0:
            return 0.0
        return float((ema_fast - ema_slow) / abs(ema_slow))

    def _score(self, rsi_value: float) -> float:
        depth = max(
            0.0, (rsi_value - self.RSI_SHORT_MIN) / (100.0 - self.RSI_SHORT_MIN)
        )
        return round(1.0 + depth, 4)

    def _stop_loss_pct(self, atr: float, entry_price: float) -> float:
        if entry_price <= 0:
            return 0.0
        pct = (self.ATR_STOP_MULT * atr / entry_price) * 100.0
        return round_numbers(min(max(pct, 0.0), 101.0), 4)

    @classmethod
    def _risk_profile_allows(
        cls,
        *,
        direction: Position,
        context: LiveMarketContext | None,
        features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"
        if context.market_stress_score >= cls.MAX_MARKET_STRESS_SCORE:
            return False, "market_stress_too_high"

        if features is not None and features.atr_pct > cls.MAX_SYMBOL_ATR_PCT:
            return False, "symbol_atr_too_high"

        if direction == Position.long:
            if (
                context.short_regime_score
                > context.long_regime_score + cls.MAX_CONTEXT_COUNTER_TREND_GAP
            ):
                return False, "market_context_short_edge"
            if context.btc_regime_score < -0.15 and context.btc_return < 0:
                return False, "btc_context_down"
            if features is not None:
                if features.micro_regime_transition == "BREAKDOWN":
                    return False, "symbol_breakdown"
                if features.micro_regime == "TREND_DOWN" and features.trend_score < 0:
                    return False, "symbol_trend_down"
            return True, "risk_profile_allows_long"

        if (
            context.long_regime_score
            > context.short_regime_score + cls.MAX_CONTEXT_COUNTER_TREND_GAP
        ):
            return False, "market_context_long_edge"
        if context.btc_regime_score > 0.15 and context.btc_return > 0:
            return False, "btc_context_up"
        if features is not None:
            if features.micro_regime_transition == "BREAKOUT_UP":
                return False, "symbol_breakout_up"
            if features.micro_regime == "TREND_UP" and features.trend_score > 0:
                return False, "symbol_trend_up"
        return True, "risk_profile_allows_short"

    def _already_emitted(self, candle_open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_candle == candle_open_time
        return self.strategy_cooldowns.get((self.ALGO, self.symbol)) == candle_open_time

    def _mark_emitted(self, candle_open_time: int) -> None:
        self._last_emitted_candle = candle_open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = candle_open_time

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            logging.info("%s skipped: market_type_not_futures", self.ALGO)
            return

        df = self.ti.df_15m
        if "ATR" not in df.columns:
            logging.info("%s skipped: atr_column_unavailable", self.ALGO)
            return

        candidate = df.iloc[-1]
        candidate_open_time = int(candidate["open_time"])

        rsi_series = self._rsi(df["close"])
        rsi_value = float(rsi_series.iloc[-1])
        previous_rsi = float(rsi_series.iloc[-2])
        volume = float(candidate["volume"])
        volume_ma = float(df["volume"].rolling(self.VOLUME_MA_WINDOW).mean().iloc[-1])
        atr = float(df["ATR"].iloc[-1])
        atr_ma = float(df["ATR"].rolling(self.ATR_MA_WINDOW).mean().iloc[-1])
        trend_score = self._trend_score(df["close"])

        if not all(
            isfinite(v)
            for v in (rsi_value, previous_rsi, volume_ma, atr, atr_ma, trend_score)
        ):
            logging.info("%s skipped: indicators_not_ready", self.ALGO)
            return

        direction, entry_reason = self._resolve_entry(
            close=float(candidate["close"]),
            open_=float(candidate["open"]),
            high=float(candidate["high"]),
            low=float(candidate["low"]),
            bb_high=float(bb_high),
            rsi_value=rsi_value,
            previous_rsi=previous_rsi,
            volume=volume,
            volume_ma=volume_ma,
            atr=atr,
            atr_ma=atr_ma,
        )
        if direction is None:
            logging.info("%s skipped: %s", self.ALGO, entry_reason)
            return
        if trend_score > self.MAX_TREND_SCORE:
            logging.info("%s skipped: trend_score_above_max", self.ALGO)
            return

        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        risk_allowed, risk_reason = self._risk_profile_allows(
            direction=direction,
            context=context,
            features=symbol_features,
        )
        if not risk_allowed:
            logging.info("%s skipped: %s", self.ALGO, risk_reason)
            return
        assert context is not None

        if self._already_emitted(candidate_open_time):
            logging.info("%s skipped: candle_already_emitted", self.ALGO)
            return

        entry_price = float(current_price)
        stop_loss_pct = self._stop_loss_pct(atr, entry_price)
        if stop_loss_pct > self.MAX_ENTRY_STOP_LOSS_PCT:
            logging.info("%s skipped: entry_stop_loss_too_wide", self.ALGO)
            return
        self._mark_emitted(candidate_open_time)
        direction_label = direction.value.upper()
        score = self._score(rsi_value)

        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            MarketType.FUTURES,
            self.symbol,
        )

        value = SignalsConsumer(
            direction=direction_label,
            autotrade=True,
            current_price=entry_price,
            volume=volume,
            score=score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=direction,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=False,
                fiat_order_size=self.MAX_FIAT_ORDER_SIZE,
                stop_loss=stop_loss_pct,
                take_profit=self.TAKE_PROFIT_PCT,
                trailing=False,
                margin_short_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        base_asset = self.current_symbol_data.base_asset
        quote_asset = self.current_symbol_data.quote_asset
        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {direction_label} ENTRY
            - Current price: {round_numbers(entry_price, decimals=self.price_precision)}
            - Entry setup: {entry_reason}
            - RSI: {round_numbers(rsi_value, 2)}
            - Bollinger lower/mid/upper: {round_numbers(bb_low, decimals=self.price_precision)} / {round_numbers(bb_mid, decimals=self.price_precision)} / {round_numbers(bb_high, decimals=self.price_precision)}
            - Volume: {round_numbers(volume, decimals=self.price_precision)} {base_asset} (20-bar avg {round_numbers(volume_ma, decimals=self.price_precision)})
            - ATR: {round_numbers(atr, decimals=self.price_precision)} {quote_asset} (20-bar avg {round_numbers(atr_ma, decimals=self.price_precision)})
            - Risk profile: {risk_reason}, max margin {self.MAX_FIAT_ORDER_SIZE} {quote_asset}
            - EMA trend score: {round_numbers(trend_score, 5)} (maximum {self.MAX_TREND_SCORE})
            - Rule intent: short-only mean-reversion fade outside the upper Bollinger Band when trend continuation risk is limited
            - Stop intent: ATR-sized emergency stop (~{stop_loss_pct}%), capped at {self.MAX_ENTRY_STOP_LOSS_PCT}%
            - Take profit intent: fixed {self.TAKE_PROFIT_PCT}% target; maximum holding intent {self.MAX_HOLDING_BARS} candles
            - Confidence score: {score}
            - Signal timestamp: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        await self.ti.dispatch_signal_record(
            value=value,
            indicators={
                "entry_reason": entry_reason,
                "rsi": rsi_value,
                "previous_rsi": previous_rsi,
                "bb_low": bb_low,
                "bb_mid": bb_mid,
                "bb_high": bb_high,
                "volume": volume,
                "volume_ma": volume_ma,
                "atr": atr,
                "atr_ma": atr_ma,
                "trend_score": trend_score,
                "max_trend_score": self.MAX_TREND_SCORE,
                "stop_loss_pct": stop_loss_pct,
                "take_profit_pct": self.TAKE_PROFIT_PCT,
                "max_holding_bars": self.MAX_HOLDING_BARS,
                "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
                "risk_reason": risk_reason,
                "market_stress_score": context.market_stress_score,
                "long_regime_score": context.long_regime_score,
                "short_regime_score": context.short_regime_score,
                "btc_regime_score": context.btc_regime_score,
                "btc_return": context.btc_return,
                "symbol_micro_regime": (
                    symbol_features.micro_regime if symbol_features else None
                ),
                "symbol_micro_regime_transition": (
                    symbol_features.micro_regime_transition if symbol_features else None
                ),
                "candidate_open_time": candidate_open_time,
            },
        )
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

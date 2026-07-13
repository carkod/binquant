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

from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class MeanReversionFade:
    """
    Pure mean-reversion fade of RSI + Bollinger Band extremes.

    No trend/regime filter — bet on a snap-back to the mean, not
    continuation. Validated via a real-data P&L backtest (400 KuCoin futures
    symbols, ~10 days of 15m candles): 220 trades, 44.5% win rate, profit
    factor 1.34, +0.41%/trade net of fees.

    Entry (long; short is the mirror):
        - RSI(14) <= RSI_LONG_MAX (oversold)
        - close <= lower Bollinger Band
        - confirmation candle: close > open (green)
        - volume >= VOLUME_RATIO_MIN * its 20-bar moving average
        - ATR(14) not in a volatility blowup (< ATR_SPIKE_MAX * its own
          20-bar average) — skip chaotic/parabolic conditions

    Exit is managed dynamically by binbot (dynamic_trailing=True): an
    ATR-sized emergency stop-loss plus an RSI "thesis invalidation" early
    exit, and a Bollinger mid-band reversion take-profit target.

    RSI here is computed inline with Wilder/EWM smoothing to match exactly
    what was backtested — pybinbot's shared `Indicators.rsi` column uses a
    simple rolling mean instead, which would silently shift these thresholds.
    ATR and the Bollinger bands DO reuse the shared `df_15m` columns (their
    formulas match the backtest exactly).
    """

    ALGO = "mean_reversion_fade"
    MIN_CANDLES = 40
    CANDLE_INTERVAL_MS = 15 * 60 * 1000

    RSI_WINDOW = 14
    RSI_LONG_MAX = 25.0
    RSI_SHORT_MIN = 75.0

    VOLUME_MA_WINDOW = 20
    VOLUME_RATIO_MIN = 1.0

    ATR_WINDOW = 14
    ATR_MA_WINDOW = 20
    ATR_SPIKE_MAX = 2.0
    ATR_STOP_MULT = 2.0

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
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        return 100 - (100 / (1 + rs))

    def _resolve_entry(
        self,
        *,
        close: float,
        open_: float,
        bb_low: float,
        bb_high: float,
        rsi_value: float,
        volume: float,
        volume_ma: float,
        atr: float,
        atr_ma: float,
    ) -> tuple[Position | None, str]:
        if atr >= self.ATR_SPIKE_MAX * atr_ma:
            return None, "atr_volatility_spike"
        if volume < self.VOLUME_RATIO_MIN * volume_ma:
            return None, "volume_below_average"

        if rsi_value <= self.RSI_LONG_MAX and close <= bb_low and close > open_:
            return Position.long, "lower_band_rsi_oversold_green"

        if rsi_value >= self.RSI_SHORT_MIN and close >= bb_high and close < open_:
            return Position.short, "upper_band_rsi_overbought_red"

        return None, "no_fade_setup"

    def _score(self, rsi_value: float, position: Position) -> float:
        if position == Position.long:
            depth = max(0.0, (self.RSI_LONG_MAX - rsi_value) / self.RSI_LONG_MAX)
        else:
            depth = max(
                0.0, (rsi_value - self.RSI_SHORT_MIN) / (100.0 - self.RSI_SHORT_MIN)
            )
        return round(1.0 + depth, 4)

    def _stop_loss_pct(self, atr: float, entry_price: float) -> float:
        if entry_price <= 0:
            return 0.0
        pct = (self.ATR_STOP_MULT * atr / entry_price) * 100.0
        return round_numbers(min(max(pct, 0.0), 101.0), 4)

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
        required_columns = {"open_time", "open", "high", "low", "close", "volume"}
        if not required_columns.issubset(df.columns):
            logging.info("%s skipped: missing_required_columns", self.ALGO)
            return
        if len(df) < self.MIN_CANDLES:
            logging.info("%s skipped: insufficient_candle_history", self.ALGO)
            return
        if df[list(required_columns)].tail(self.MIN_CANDLES).isnull().any().any():
            logging.info("%s skipped: null_candle_history", self.ALGO)
            return
        if "ATR" not in df.columns:
            logging.info("%s skipped: atr_column_unavailable", self.ALGO)
            return

        candidate = df.iloc[-1]
        candidate_open_time = int(candidate["open_time"])

        rsi_value = float(self._rsi(df["close"]).iloc[-1])
        volume = float(candidate["volume"])
        volume_ma = float(df["volume"].rolling(self.VOLUME_MA_WINDOW).mean().iloc[-1])
        atr = float(df["ATR"].iloc[-1])
        atr_ma = float(df["ATR"].rolling(self.ATR_MA_WINDOW).mean().iloc[-1])

        if not all(isfinite(v) for v in (rsi_value, volume_ma, atr, atr_ma)):
            logging.info("%s skipped: indicators_not_ready", self.ALGO)
            return

        direction, entry_reason = self._resolve_entry(
            close=float(candidate["close"]),
            open_=float(candidate["open"]),
            bb_low=float(bb_low),
            bb_high=float(bb_high),
            rsi_value=rsi_value,
            volume=volume,
            volume_ma=volume_ma,
            atr=atr,
            atr_ma=atr_ma,
        )
        if direction is None:
            logging.info("%s skipped: %s", self.ALGO, entry_reason)
            return

        if self._already_emitted(candidate_open_time):
            logging.info("%s skipped: candle_already_emitted", self.ALGO)
            return
        self._mark_emitted(candidate_open_time)

        entry_price = float(current_price)
        stop_loss_pct = self._stop_loss_pct(atr, entry_price)
        direction_label = direction.value.upper()
        score = self._score(rsi_value, direction)

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
                dynamic_trailing=True,
                stop_loss=stop_loss_pct,
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
            - Rule intent: pure mean-reversion fade of a Bollinger extreme, no trend/regime filter
            - Stop intent: ATR-sized emergency stop (~{stop_loss_pct}%), tightened early on RSI thesis invalidation
            - Take profit intent: reversion to the 20-bar Bollinger mid-band, managed dynamically
            - Confidence score: {score}
            - Signal timestamp: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        self.ti.dispatch_signal_record(
            value=value,
            indicators={
                "entry_reason": entry_reason,
                "rsi": rsi_value,
                "bb_low": bb_low,
                "bb_mid": bb_mid,
                "bb_high": bb_high,
                "volume": volume,
                "volume_ma": volume_ma,
                "atr": atr,
                "atr_ma": atr_ma,
                "stop_loss_pct": stop_loss_pct,
                "candidate_open_time": candidate_open_time,
            },
        )
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

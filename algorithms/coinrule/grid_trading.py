import logging
import os
from typing import TYPE_CHECKING

from pybinbot import HABollinguerSpread, SignalsConsumer, Strategy, round_numbers

from market_regime.regime_routing import (
    resolve_symbol_features,
    supports_grid_trading,
)
from models.algorithms import GridSignalDecision
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class GridTrading:
    """
    Entry-only range-aware Coinrule-style grid logic for confirmed range markets.

    This strategy only looks for long-side grid entries when the broader market
    and the symbol are both behaving as range-bound, non-transitional markets.
    Transitional regimes are intentionally excluded for this algorithm. Exit
    logic, profit-taking, stop-losses, and position management are handled
    elsewhere in the system.

    Intent:
    - detect a stable sideways/range-bound market
    - detect weakness near the lower edge of the range
    - emit a long-entry signal
    """

    BUY_TRIGGER_PCT = 0.02
    CLIP_SIZE_QUOTE = 20.0
    MAX_RUNS = 10

    LOOKBACK_CANDLES = 48
    MIN_RANGE_WIDTH = 0.015
    MAX_RANGE_WIDTH = 0.08
    MAX_RANGE_DRIFT = 0.03
    MIN_BB_WIDTH = 0.01
    MAX_BB_WIDTH = 0.08
    MAX_TREND_SLOPE_PROXY = 0.012

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self.df_15m = cls.df_15m
        self.config = cls.config
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context

    def evaluate(
        self,
        recent_window,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> GridSignalDecision:
        recent_high = float(recent_window["high"].max())
        recent_low = float(recent_window["low"].min())
        first_close = float(recent_window["close"].iloc[0])
        last_close = float(recent_window["close"].iloc[-1])
        rsi_value = float(recent_window["rsi"].iloc[-1])

        if recent_low <= 0 or first_close <= 0 or bb_mid == 0:
            return GridSignalDecision(
                should_trigger=False,
                reason="Invalid denominator in range or Bollinger calculations.",
            )

        range_width = (recent_high - recent_low) / recent_low
        range_drift = abs((last_close - first_close) / first_close)
        bb_width = (bb_high - bb_low) / bb_mid if bb_high > bb_low else 0.0

        if bb_high == bb_low:
            band_position = 0.5
        else:
            band_position = (current_price - bb_low) / (bb_high - bb_low)

        rolling_mean = recent_window["close"].rolling(8).mean().dropna()
        if len(rolling_mean) >= 2:
            trend_slope_proxy = abs(
                (float(rolling_mean.iloc[-1]) - float(rolling_mean.iloc[0]))
                / first_close
            )
        else:
            trend_slope_proxy = 0.0

        is_sideways = (
            self.MIN_RANGE_WIDTH <= range_width <= self.MAX_RANGE_WIDTH
            and range_drift <= self.MAX_RANGE_DRIFT
            and self.MIN_BB_WIDTH <= bb_width <= self.MAX_BB_WIDTH
            and trend_slope_proxy <= self.MAX_TREND_SLOPE_PROXY
        )

        if not is_sideways:
            return GridSignalDecision(
                should_trigger=False,
                reason=(
                    "Market rejected: not sufficiently sideways "
                    f"(range_width={range_width:.4f}, "
                    f"range_drift={range_drift:.4f}, "
                    f"bb_width={bb_width:.4f}, "
                    f"trend_proxy={trend_slope_proxy:.4f})."
                ),
                range_width=range_width,
                range_drift=range_drift,
                bb_width=bb_width,
                band_position=band_position,
                rsi_value=rsi_value,
                trend_slope_proxy=trend_slope_proxy,
            )

        live_anchor = float(recent_window["close"].iloc[-2])
        move_from_anchor = (
            (current_price - live_anchor) / live_anchor if live_anchor > 0 else 0.0
        )

        buy_zone = (
            move_from_anchor <= -self.BUY_TRIGGER_PCT
            and rsi_value < 35
            and band_position <= 0.4
            and current_price <= bb_mid
        )

        if buy_zone:
            return GridSignalDecision(
                should_trigger=True,
                action="buy",
                reason=(
                    "Sideways market confirmed and price is in a lower-band grid "
                    f"entry zone after a {move_from_anchor * 100:.2f}% move from "
                    "the live anchor."
                ),
                trigger_move_pct=self.BUY_TRIGGER_PCT,
                range_width=range_width,
                range_drift=range_drift,
                bb_width=bb_width,
                band_position=band_position,
                rsi_value=rsi_value,
                trend_slope_proxy=trend_slope_proxy,
            )

        return GridSignalDecision(
            should_trigger=False,
            reason=(
                "Sideways market confirmed, but price is not in a valid lower-band "
                "grid entry zone."
            ),
            trigger_move_pct=self.BUY_TRIGGER_PCT,
            range_width=range_width,
            range_drift=range_drift,
            bb_width=bb_width,
            band_position=band_position,
            rsi_value=rsi_value,
            trend_slope_proxy=trend_slope_proxy,
        )

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        """
        Entry-only grid trading signal for stable range-bound markets.

        Best when the market is chopping sideways and price repeatedly
        oscillates around a stable band. This module only emits the long-entry
        side of the grid logic, and it explicitly rejects transitional market
        states. Exit behavior is delegated to another part of the system.
        """
        context = self.latest_market_context
        if not supports_grid_trading(context=context, symbol=self.symbol):
            logging.info("Grid skipped: regime router does not support grid trading.")
            return

        symbol_features = resolve_symbol_features(context, self.symbol)
        if context is not None and context.market_regime != "RANGE":
            logging.info(
                "Grid skipped: algorithm requires RANGE market regime, got %s.",
                context.market_regime,
            )
            return

        if symbol_features is not None and symbol_features.micro_regime != "RANGE":
            logging.info(
                "Grid skipped: algorithm requires RANGE symbol regime, got %s.",
                symbol_features.micro_regime,
            )
            return

        self.df_15m = self.ti.df_15m.copy()
        if len(self.df_15m) < self.LOOKBACK_CANDLES:
            logging.warning(
                "15m candles grid trading not enough data for symbol: %s",
                self.symbol,
            )
            return

        required_cols = ["high", "low", "close", "rsi"]
        recent_window = self.df_15m.tail(self.LOOKBACK_CANDLES)
        if recent_window[required_cols].isnull().any().any():
            logging.info("Grid skipped: recent data contains nulls.")
            return

        decision = self.evaluate(
            recent_window=recent_window,
            current_price=float(current_price),
            bb_high=float(bb_high),
            bb_mid=float(bb_mid),
            bb_low=float(bb_low),
        )

        if not decision.should_trigger or decision.action is None:
            logging.info("Grid skipped: %s", decision.reason)
            return

        algo = "coinrule_grid_trading"
        bot_strategy = Strategy.long
        autotrade = False

        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        action_text = (
            f"BUY ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} with USDT wallet "
            f"as market order after a -{self.BUY_TRIGGER_PCT * 100:.1f}% move"
        )

        msg = f"""
            - [{os.getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, 6)}
            - Strategy: {bot_strategy.value}
            - Market mode: Sideways / range-bound
            - Market regime: {context.market_regime if context is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Grid logic: Entry-only fixed-clip grid in sideways markets
            - Action: LONG ENTRY
            - Rule intent: {action_text}
            - Max runs configured: {self.MAX_RUNS}
            - RSI: {round_numbers(decision.rsi_value, 2)}
            - Range width ({self.LOOKBACK_CANDLES} candles): {round_numbers(decision.range_width * 100, 2)}%
            - Range drift ({self.LOOKBACK_CANDLES} candles): {round_numbers(decision.range_drift * 100, 2)}%
            - Bollinger width: {round_numbers(decision.bb_width * 100, 2)}%
            - Band position: {round_numbers(decision.band_position, 3)}
            - Trend proxy: {round_numbers(decision.trend_slope_proxy * 100, 2)}%
            - Reason: {decision.reason}
            - {"Autotrade is disabled for testing" if not autotrade else "Autotrade is enabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            symbol=self.symbol,
            algo=algo,
            bot_strategy=bot_strategy,
            market_type=self.market_type,
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

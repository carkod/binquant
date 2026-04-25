import logging
import os
from typing import TYPE_CHECKING

from pybinbot import Position, round_numbers

from market_regime.regime_routing import resolve_symbol_features
from models.strategies import GridSignalDecision
from shared.strategy_mixin import StrategyMixin
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class GridTrading(StrategyMixin):
    """
    Simple Coinrule-style manual grid logic.

    The strategy compares the current live price against the latest anchor
    close and emits a manual alert whenever price moves by +/-2%. This keeps
    the rule easy to inspect while autotrade is intentionally disabled.
    """

    ALGO = "coinrule_grid_trading"
    CLIP_SIZE_QUOTE = 20.0
    LEVERAGE = 3
    LOOKBACK_CANDLES = 2

    def __init__(
        self,
        cls: "ContextEvaluator",
        buy_trigger_pct: float = 0.02,
        sell_trigger_pct: float = 0.02,
    ) -> None:
        self.ti = cls
        self.df_15m = cls.df_15m
        self.config = cls.config
        self.binbot_api = cls.binbot_api
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.latest_market_context = cls.latest_market_context
        self.buy_trigger_pct = buy_trigger_pct
        self.sell_trigger_pct = sell_trigger_pct

    @property
    def latest_market_context(self):
        return self.ti.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value) -> None:
        self.ti.latest_market_context = value

    def evaluate(
        self,
        recent_window,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> GridSignalDecision:
        del bb_high, bb_mid, bb_low
        live_anchor = float(recent_window["close"].iloc[-2])
        rsi_value = (
            float(recent_window["rsi"].iloc[-1])
            if "rsi" in recent_window.columns
            else 50.0
        )

        if live_anchor <= 0:
            return GridSignalDecision(
                should_trigger=False,
                reason="Invalid live anchor for percentage move calculation.",
            )

        move_from_anchor = (
            (current_price - live_anchor) / live_anchor if live_anchor > 0 else 0.0
        )

        buy_zone = move_from_anchor <= -self.buy_trigger_pct
        sell_zone = move_from_anchor >= self.sell_trigger_pct

        if buy_zone:
            return GridSignalDecision(
                should_trigger=True,
                action="buy",
                reason=(
                    "Price is down enough from the live anchor to trigger the "
                    f"manual BUY leg ({move_from_anchor * 100:.2f}%)."
                ),
                trigger_move_pct=self.buy_trigger_pct,
                rsi_value=rsi_value,
            )

        if sell_zone:
            return GridSignalDecision(
                should_trigger=True,
                action="sell",
                reason=(
                    "Price is up enough from the live anchor to trigger the "
                    f"manual SELL leg ({move_from_anchor * 100:.2f}%)."
                ),
                trigger_move_pct=self.sell_trigger_pct,
                rsi_value=rsi_value,
            )

        return GridSignalDecision(
            should_trigger=False,
            reason=(
                "Price has not moved far enough from the live anchor for the "
                "manual +/-2% grid rule."
            ),
            trigger_move_pct=self.buy_trigger_pct,
            rsi_value=rsi_value,
        )

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        """
        Manual-only Coinrule grid signal based on a simple +/-2% move.
        """
        context = self.latest_market_context
        if context is None:
            logging.info(
                "Grid skipped: market context unavailable, RANGE/RANGE match required."
            )
            return

        symbol_features = resolve_symbol_features(context, self.symbol)
        if symbol_features is None:
            logging.info(
                "Grid skipped: symbol regime unavailable, RANGE/RANGE match required."
            )
            return

        if context.market_regime != "RANGE" or symbol_features.micro_regime != "RANGE":
            logging.info(
                "Grid skipped: requires RANGE market and RANGE coin regime, got market=%s coin=%s.",
                context.market_regime,
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

        required_cols = ["close"]
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

        autotrade = False
        active_bots = self.get_active_bots(algo=self.ALGO, symbol=self.symbol)
        if len(active_bots) > 0:
            id = active_bots[0]["id"]
            self.deactivate_active_bot(
                bot_id=id,
                symbol=self.symbol,
                source_label="Grid Trading exit",
            )
            self.binbot_api.submit_bot_event_logs(
                bot_id=id,
                message=[
                    f"Deactivated active bot from Grid Trading exit before new {decision.action} entry."
                ],
            )
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        live_anchor = float(recent_window["close"].iloc[-2])
        move_from_anchor = (
            (float(current_price) - live_anchor) / live_anchor
            if live_anchor > 0
            else 0.0
        )

        if decision.action == "sell":
            action_label = "SHORT SELL ALERT"
            bot_strategy = Position.short
            grid_logic = f"Simple +{self.sell_trigger_pct * 100:.1f}% manual contract sell trigger"
            action_text = (
                f"SELL ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} as market order "
                f"with {self.LEVERAGE}x leverage after a +{self.sell_trigger_pct * 100:.1f}% move "
                "from the live anchor"
            )

            msg = f"""
            - [{os.getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {action_label}
            - Current price: {round_numbers(current_price, 6)}
            - Live anchor price: {round_numbers(live_anchor, 6)}
            - Move from live anchor: {round_numbers(move_from_anchor * 100, 2)}%
            - Strategy: {bot_strategy.value}
            - Rule intent: {action_text}
            - Market regime: {context.market_regime if context is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Grid logic: {grid_logic}
            - Order setup: market order, isolated margin, {self.LEVERAGE}x leverage
            - RSI: {round_numbers(decision.rsi_value, 2)}
            - Reason: {decision.reason}
            - Autotrade route: manual_only
            - Autotrade is disabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """
            await self.telegram_consumer.send_signal(msg)
            return

        action_label = "LONG BUY ALERT"
        bot_strategy = Position.long
        grid_logic = (
            f"Simple -{self.buy_trigger_pct * 100:.1f}% manual contract buy trigger"
        )
        action_text = (
            f"BUY ${self.CLIP_SIZE_QUOTE:.2f} of {self.symbol} as market order "
            f"using isolated margin with {self.LEVERAGE}x leverage after a "
            f"-{self.buy_trigger_pct * 100:.1f}% move from the live anchor"
        )

        msg = f"""
            - [{os.getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: {action_label}
            - Current price: {round_numbers(current_price, 6)}
            - Live anchor price: {round_numbers(live_anchor, 6)}
            - Move from live anchor: {round_numbers(move_from_anchor * 100, 2)}%
            - Strategy: {bot_strategy.value}
            - Rule intent: {action_text}
            - Market regime: {context.market_regime if context is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context is not None and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features is not None and symbol_features.micro_regime_transition is not None else "None"}
            - Grid logic: {grid_logic}
            - Order setup: market order, isolated margin, {self.LEVERAGE}x leverage
            - RSI: {round_numbers(decision.rsi_value, 2)}
            - Reason: {decision.reason}
            - Autotrade route: manual_only
            - {"Autotrade is disabled" if not autotrade else "Autotrade is enabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        await self.telegram_consumer.send_signal(msg)

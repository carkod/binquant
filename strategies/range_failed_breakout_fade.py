import logging
from os import getenv
from typing import TYPE_CHECKING

from pybinbot import (
    BotBase,
    HABollinguerSpread,
    Position,
    SignalsConsumer,
    round_numbers,
)

from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class RangeFailedBreakoutFade:
    """
    Fade the failed breakout in RANGE.

    Reuses the spike_hunter v3 breakout detector to find bullish spike
    candidates (volume cluster / price break / acceleration on an upward
    streak) and shorts them when the broader market is *not* rallying:

        - market_regime == RANGE
        - context.average_return < -0.01 (broad market failing to rally)
        - symbol relative_strength_vs_btc >= 0 (outperformer during selloff)

    The thesis is that in a sleepy/red market, fresh breakouts on
    outperformers are exhaustion plays rather than momentum starts —
    they tend to fade hardest on reversal.
    """

    AVG_RETURN_MAX = -0.01

    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.config = cls.config
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.exchange = cls.exchange
        self.market_type = cls.market_type
        self.binbot_api = cls.binbot_api
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision

    @property
    def latest_market_context(self) -> LiveMarketContext | None:
        return self.ti.latest_market_context

    def regime_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"
        if context.market_regime is None:
            return False, "market_regime_unavailable"
        if context.market_regime != "RANGE":
            return False, f"market_regime_{context.market_regime.lower()}"
        if context.average_return >= self.AVG_RETURN_MAX:
            return False, "market_avg_return_not_failing"
        if symbol_features is None:
            return False, "symbol_regime_unavailable"
        if symbol_features.relative_strength_vs_btc < 0:
            return False, "symbol_underperforming_btc"
        return True, "range_failed_breakout_fade"

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        algo = "range_failed_breakout_fade"

        last_spike = self.ti.sh3.latest_signal()
        if not last_spike:
            return

        long_flags = (
            last_spike["cumulative_price_break_flag"]
            or last_spike["volume_cluster_flag"]
            or last_spike["accel_spike_flag"]
        )
        if not (long_flags and last_spike["upward"]):
            return

        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        should_emit, route_reason = self.regime_routing(
            context=context,
            symbol_features=symbol_features,
        )
        if not should_emit:
            return

        # Per request: keep autotrade off while telemetry on this fade
        # variant is collected before going live.
        autotrade = False
        bot_strategy = Position.short

        base_asset = self.current_symbol_data["base_asset"]
        quote_asset = self.current_symbol_data["quote_asset"]
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        value = SignalsConsumer(
            direction="SHORT",
            autotrade=autotrade,
            current_price=current_price,
            bot_params=BotBase(
                pair=self.symbol,
                name=algo,
                position=bot_strategy,
                market_type=self.market_type,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        msg = f"""
            - 📉 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: SHORT ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: fade a bullish spike that fires while the broader market is failing to rally and this symbol is leading the tape — likely exhaustion, not momentum.
            - Candle time: {last_spike["timestamp"]}
            - Volume: {round_numbers(last_spike["volume"], decimals=self.price_precision)} {base_asset}
            - Quote volume: {round_numbers(last_spike["quote_asset_volume"], decimals=self.price_precision)} {quote_asset}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market avg return: {round_numbers(context.average_return, 4) if context else "UNAVAILABLE"}
            - Relative strength vs BTC: {round_numbers(symbol_features.relative_strength_vs_btc, 4) if symbol_features else "UNAVAILABLE"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Autotrade route: {route_reason}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
            """

        self.ti.dispatch_signal_record(value=value)
        self.telegram_consumer.dispatch_signal(msg)
        if autotrade:
            await self.at_consumer.process_autotrade_restrictions(value)
        else:
            logging.info(
                "[%s] short signal emitted for %s (autotrade disabled, route=%s)",
                algo,
                self.symbol,
                route_reason,
            )

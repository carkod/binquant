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


class RelativeStrengthReversalRange:
    """
    Contrarian long on the relative-strength leader during a broad-market
    selloff in a RANGE regime.

    Entry conditions:
        - market_regime == RANGE
        - context.average_return < -0.02 (broad market clearly red)
        - symbol relative_strength_vs_btc > +0.05 (markedly outperforming)
        - current 15m volume above the 20th percentile of the recent
          window (i.e. not a dead-tape print)

    Thesis: strength divergence during weakness suggests accumulation /
    reduced forced-seller exposure; reversal carries lower tail risk
    than picking up coins being liquidated.
    """

    AVG_RETURN_MAX = -0.02
    RS_VS_BTC_MIN = 0.05
    VOLUME_PERCENTILE = 0.20
    VOLUME_WINDOW = 96  # 24h on 15m bars

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
            return False, "market_avg_return_not_weak_enough"
        if symbol_features is None:
            return False, "symbol_regime_unavailable"
        if symbol_features.relative_strength_vs_btc <= self.RS_VS_BTC_MIN:
            return False, "symbol_rs_vs_btc_insufficient"
        return True, "range_rs_reversal"

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        algo = "relative_strength_reversal_range"

        df = self.ti.df_15m
        if df is None or df.empty or len(df) < self.VOLUME_WINDOW:
            return

        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        should_emit, route_reason = self.regime_routing(
            context=context,
            symbol_features=symbol_features,
        )
        if not should_emit:
            return

        recent_volume = df["volume"].tail(self.VOLUME_WINDOW)
        volume_floor = float(recent_volume.quantile(self.VOLUME_PERCENTILE))
        current_volume = float(df["volume"].iloc[-1])
        if current_volume <= volume_floor:
            return

        # Per request pattern: keep autotrade off while telemetry on this
        # contrarian variant is collected before going live.
        autotrade = False
        bot_strategy = Position.long

        base_asset = self.current_symbol_data["base_asset"]
        quote_asset = self.current_symbol_data["quote_asset"]
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        quote_volume = (
            float(df["quote_asset_volume"].iloc[-1])
            if "quote_asset_volume" in df.columns
            else 0.0
        )

        value = SignalsConsumer(
            direction="LONG",
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
            - 📈 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: contrarian long on a relative-strength leader holding up while the broader market sells off — accumulation read, not chase.
            - Volume: {round_numbers(current_volume, decimals=self.price_precision)} {base_asset}
            - Volume 20th pct: {round_numbers(volume_floor, decimals=self.price_precision)} {base_asset}
            - Quote volume: {round_numbers(quote_volume, decimals=self.price_precision)} {quote_asset}
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
                "[%s] long signal emitted for %s (autotrade disabled, route=%s)",
                algo,
                self.symbol,
                route_reason,
            )

from os import getenv
from typing import TYPE_CHECKING
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    KlineSchema,
    Position,
    SignalsConsumer,
    round_numbers,
)
from pandera.typing import DataFrame as TypedDataFrame
from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LiquidationSweepPump:
    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.config = cls.config
        # Symbol / context
        self.symbol = cls.symbol
        self.kucoin_symbol = cls.kucoin_symbol
        self.exchange = cls.exchange
        self.binbot_api = cls.binbot_api
        self.telegram_consumer = cls.telegram_consumer
        self.market_type = cls.market_type
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.qty_precision = cls.qty_precision
        self.oi_growth = cls.oi_data
        self.latest_market_context = cls.latest_market_context

    @property
    def latest_market_context(self) -> LiveMarketContext | None:
        return self.ti.latest_market_context

    @latest_market_context.setter
    def latest_market_context(self, value: LiveMarketContext | None) -> None:
        self.ti.latest_market_context = value

    @staticmethod
    def _has_bullish_transitional_market(context: LiveMarketContext) -> bool:
        if context.market_regime != "TRANSITIONAL":
            return False
        if context.market_stress_score >= 0.35:
            return False
        return context.long_tailwind > 0 and context.long_regime_score > max(
            context.short_regime_score,
            context.range_regime_score,
            context.stress_regime_score,
        )

    @staticmethod
    def _has_bullish_transitional_symbol(features: SymbolMarketFeatures) -> bool:
        if features.micro_regime != "TRANSITIONAL":
            return False
        return (
            features.trend_score > 0
            and features.above_ema20
            and features.relative_strength_vs_btc >= 0
        )

    def regime_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"

        if context.market_stress_score >= 0.35:
            return False, "market_stress_too_high"

        if context.market_regime is None:
            return False, "market_regime_unavailable"

        if context.market_regime == "TREND_UP":
            market_route = "market_trend_up"
        elif self._has_bullish_transitional_market(context):
            market_route = "market_transitional_bullish"
        else:
            return False, f"market_regime_{context.market_regime.lower()}"

        if symbol_features is None:
            return False, "symbol_regime_unavailable"

        if symbol_features.micro_regime == "TREND_UP":
            return True, f"{market_route}_symbol_trend_up"

        if self._has_bullish_transitional_symbol(symbol_features):
            return True, f"{market_route}_symbol_transitional_bullish"

        if symbol_features.micro_regime is None:
            return False, "symbol_regime_unavailable"
        return False, f"symbol_regime_{symbol_features.micro_regime.lower()}"

    def compute_pump_score(
        self, df: TypedDataFrame[KlineSchema], window_hours=3
    ) -> TypedDataFrame[KlineSchema]:
        """
        Compute pump score using:
        - Relative volume
        - Early momentum
        - Price compression
        - OI growth (cached per asset; defaults to 1.0 if unavailable)
        """
        df = df.copy()

        # --- 1. Relative Volume ---
        df["rel_volume"] = df.volume / df.volume.rolling(
            window=window_hours * 2
        ).mean().shift(window_hours)

        # --- 2. Early Momentum ---
        df["price_momentum"] = df.close.pct_change(periods=window_hours)

        # --- 3. Price Compression ---
        df["price_range_frac"] = (
            df.high.rolling(window=window_hours * 2).max()
            - df.low.rolling(window=window_hours * 2).min()
        ) / df.close

        # --- 4. OI Growth ---
        oi_growth = 1 + max(0, (self.oi_growth - 1)) if self.oi_growth else 1.0

        # --- 5. Pump Score ---
        df["pump_score"] = (
            df["rel_volume"] * (1 + df["price_momentum"]) * oi_growth
        ) / df["price_range_frac"]
        df["pump_score_smooth"] = df["pump_score"].rolling(window=2).mean()

        return df

    async def signal(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        """
        Generate signal if pump score exceeds threshold and OI growth filter
        """
        df = self.ti.df_15m
        df_btc = self.ti.df_btc_15m
        if df is None or df.empty:
            return None

        algo = "liquidation_sweep_pump"
        bot_strategy = Position.long
        autotrade = False
        base_asset = self.current_symbol_data["base_asset"]

        df = self.compute_pump_score(df)

        # --- Filters ---
        # Take last N candles (say 48 for 12h)
        recent_scores = df["pump_score_smooth"].iloc[-48:]
        btc_momentum = (
            df_btc.close.pct_change().iloc[-1]
            if df_btc is not None and not df_btc.empty and len(df_btc) > 1
            else 0.0
        )

        # Keep the trigger selective, but allow strong setups that land in the
        # top quintile of recent pump-score readings instead of only rarer outliers.
        PUMP_SCORE_THRESHOLD = recent_scores.quantile(0.80)
        row = df.iloc[-1]
        latest_score = row["pump_score_smooth"]
        latest_raw_score = row["pump_score"]
        trigger_score = max(float(latest_score), float(latest_raw_score))

        # Allow signals while BTC is mildly red; only block under sharper
        # market-wide selloffs that tend to invalidate breakout follow-through.
        if btc_momentum < -0.02:
            return

        if latest_score is None or trigger_score < PUMP_SCORE_THRESHOLD:
            return

        # Optional OI confirmation
        if self.oi_growth is not None and self.oi_growth < 1.02:
            return

        context = self.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        should_emit, route_reason = self.regime_routing(
            context=context,
            symbol_features=symbol_features,
        )
        if not should_emit:
            return
        autotrade = True

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        value = SignalsConsumer(
            direction="LONG",
            autotrade=autotrade,
            bot_params=BotBase(
                pair=self.symbol,
                name=algo,
                position=bot_strategy,
                market_type=self.market_type,
            ),
            current_price=current_price,
            volume=float(row.volume),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        value.autotrade = autotrade

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: BUY after a liquidation-style pump score breakout in bullish market routing
            - Score: {trigger_score:.2f}
            - Volume: {round_numbers(float(row.volume), decimals=self.price_precision)} {base_asset}
            - OI Growth: {self.oi_growth:.2f}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Autotrade route: {route_reason}
            - Market stress: {round_numbers(context.market_stress_score, 3) if context else 0}
            - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

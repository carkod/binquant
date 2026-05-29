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
    SHORT_ADP_THRESHOLD = 0.3
    LONG_ADP_THRESHOLD = -0.4
    BTC_STALLED_MOMENTUM_ABS = 0.002

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
        self.market_breadth_data = cls.market_breadth_data

    @staticmethod
    def _context_adp(context: LiveMarketContext) -> float:
        return context.advancers_ratio - context.decliners_ratio

    @staticmethod
    def _has_weak_symbol_followthrough(features: SymbolMarketFeatures) -> bool:
        return features.relative_strength_vs_btc <= 0 and (
            features.trend_score <= 0
            or not features.above_ema20
            or features.micro_regime != "TREND_UP"
        )

    def _adp_values(self, context: LiveMarketContext) -> list[float]:
        values = (self.market_breadth_data or {}).get("adp", [])
        if len(values) >= 2:
            return [float(value) for value in values]
        return [self._context_adp(context)]

    def _latest_adp(self, context: LiveMarketContext) -> float:
        return self._adp_values(context)[-1]

    def _is_breadth_falling(self, context: LiveMarketContext) -> bool:
        values = self._adp_values(context)
        return len(values) >= 2 and values[-1] < values[-2]

    def _is_breadth_increasing(self, context: LiveMarketContext) -> bool:
        values = self._adp_values(context)
        return len(values) >= 2 and values[-1] > values[-2]

    def breadth_fade_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
        btc_momentum: float,
    ) -> tuple[Position | None, str]:
        if context is None:
            return None, "market_context_unavailable"

        if context.market_stress_score >= 0.35:
            return None, "market_stress_too_high"

        adp = self._latest_adp(context)

        if adp > self.SHORT_ADP_THRESHOLD:
            if not self._is_breadth_falling(context):
                return None, "hot_breadth_not_falling"
            if abs(btc_momentum) > self.BTC_STALLED_MOMENTUM_ABS:
                return None, "btc_not_stalled"
            if symbol_features is None:
                return None, "symbol_regime_unavailable"
            if not self._has_weak_symbol_followthrough(symbol_features):
                return None, "symbol_followthrough_not_weak"
            return Position.short, "breadth_hot_fading_btc_stalled_symbol_weak"

        if adp <= self.LONG_ADP_THRESHOLD:
            if not self._is_breadth_increasing(context):
                return None, "washed_out_breadth_not_increasing"
            if btc_momentum <= 0:
                return None, "btc_not_increasing"
            return Position.long, "breadth_washed_out_recovering_btc_up"

        return None, "adp_not_extreme"

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

        if latest_score is None or trigger_score < PUMP_SCORE_THRESHOLD:
            return

        # Optional OI confirmation
        if self.oi_growth is not None and self.oi_growth < 1.02:
            return

        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        bot_strategy, route_reason = self.breadth_fade_routing(
            context=context,
            symbol_features=symbol_features,
            btc_momentum=float(btc_momentum),
        )
        if bot_strategy is None:
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )
        action_label = f"{bot_strategy.value.upper()} ENTRY"
        direction = bot_strategy.value.upper()
        rule_intent = (
            "SELL a liquidation-style pump when breadth is hot but fading, BTC is stalled, and this symbol is not following through"
            if bot_strategy == Position.short
            else "BUY a liquidation-style pump when breadth is washed out but recovering and BTC is increasing"
        )

        value = SignalsConsumer(
            direction=direction,
            autotrade=True,
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

        value.autotrade = True

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: {action_label}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: {bot_strategy.value}
            - Rule intent: {rule_intent}
            - Score: {trigger_score:.2f}
            - Volume: {round_numbers(float(row.volume), decimals=self.price_precision)} {base_asset}
            - OI Growth: {self.oi_growth:.2f}
            - ADP: {round_numbers(self._latest_adp(context), 3) if context else "UNAVAILABLE"}
            - BTC momentum: {round_numbers(float(btc_momentum), 5)}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Autotrade route: {route_reason}
            - Market stress: {round_numbers(context.market_stress_score, 3) if context else 0}
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        self.ti.dispatch_signal_record(value=value)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

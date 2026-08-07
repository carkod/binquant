from math import isfinite
from os import getenv
from typing import TYPE_CHECKING, Any
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    KlineSchema,
    MarketBreadthSeries,
    Position,
    SignalsConsumer,
    round_numbers,
    timestamp_sort_key,
)
from pandera.typing import DataFrame as TypedDataFrame
from market_regime.models import LiveMarketContext, SymbolMarketFeatures
from market_regime.regime_routing import resolve_symbol_features
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LiquidationSweepPump:
    ALGO = "liquidation_sweep_pump"
    LONG_MARKET_BREADTH_THRESHOLD = -0.4
    LONG_STOP_LOSS_PCT = 2.0
    LONG_TAKE_PROFIT_PCT = 2.5

    def __init__(self, cls: "ContextEvaluator"):
        self.ti = cls
        self.config = cls.config
        # Symbol / context
        self.symbol = cls.symbol
        self.exchange = cls.exchange
        self.telegram_consumer = cls.telegram_consumer
        self.market_type = cls.market_type
        self.at_consumer = cls.at_consumer
        self.current_symbol_data = cls.current_symbol_data
        self.price_precision = cls.price_precision
        self.oi_growth = cls.oi_data
        self.market_breadth_data: MarketBreadthSeries | None = cls.market_breadth_data

    @staticmethod
    def _context_market_breadth(context: LiveMarketContext) -> float:
        return context.advancers_ratio - context.decliners_ratio

    @staticmethod
    def _coerce_market_breadth(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if isfinite(parsed) else None

    def _market_breadth_values(self, context: LiveMarketContext) -> list[float]:
        breadth = self.market_breadth_data
        if breadth is not None and len(breadth.market_breadth) >= 2:
            timestamped_values = []
            for timestamp, value in zip(
                breadth.timestamp, breadth.market_breadth, strict=False
            ):
                sort_key = timestamp_sort_key(timestamp)
                parsed = self._coerce_market_breadth(value)
                if sort_key is not None and parsed is not None:
                    timestamped_values.append((sort_key, parsed))

            if len(timestamped_values) >= 2:
                return [
                    value
                    for _, value in sorted(timestamped_values, key=lambda item: item[0])
                ]

        return [self._context_market_breadth(context)]

    def _latest_market_breadth(self, context: LiveMarketContext) -> float:
        return self._market_breadth_values(context)[-1]

    def _is_market_breadth_increasing(self, context: LiveMarketContext) -> bool:
        values = self._market_breadth_values(context)
        return len(values) >= 2 and values[-1] > values[-2]

    def long_entry_routing(
        self,
        context: LiveMarketContext | None,
        symbol_features: SymbolMarketFeatures | None,
        btc_momentum: float,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"

        if context.market_stress_score >= 0.35:
            return False, "market_stress_too_high"

        market_breadth = self._latest_market_breadth(context)

        if market_breadth <= self.LONG_MARKET_BREADTH_THRESHOLD:
            if not self._is_market_breadth_increasing(context):
                return False, "washed_out_breadth_not_increasing"
            if btc_momentum <= 0:
                return False, "btc_not_increasing"
            if symbol_features is None:
                return False, "symbol_regime_unavailable"
            if symbol_features.trend_score <= 0:
                return False, "symbol_trend_not_up"
            return True, "breadth_washed_out_recovering_btc_up_symbol_up"

        return False, "market_breadth_not_extreme"

    def compute_pump_score(
        self, df: TypedDataFrame[KlineSchema], momentum_bars: int = 3
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
            window=momentum_bars * 2
        ).mean().shift(momentum_bars)

        # --- 2. Early Momentum ---
        df["price_momentum"] = df.close.pct_change(periods=momentum_bars)

        # --- 3. Price Compression ---
        df["price_range_frac"] = (
            df.high.rolling(window=momentum_bars * 2).max()
            - df.low.rolling(window=momentum_bars * 2).min()
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

        algo = self.ALGO
        base_asset = self.current_symbol_data.base_asset

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
        should_enter, route_reason = self.long_entry_routing(
            context=context,
            symbol_features=symbol_features,
            btc_momentum=float(btc_momentum),
        )
        if not should_enter:
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )
        bot_params = BotBase(
            pair=self.symbol,
            name=algo,
            position=Position.long,
            market_type=self.market_type,
            dynamic_trailing=False,
            stop_loss=self.LONG_STOP_LOSS_PCT,
            take_profit=self.LONG_TAKE_PROFIT_PCT,
            trailing=False,
            margin_short_reversal=False,
        )

        value = SignalsConsumer(
            direction="LONG",
            autotrade=True,
            bot_params=bot_params,
            current_price=current_price,
            volume=float(row.volume),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Strategy: long
            - Rule intent: BUY a liquidation-style pump when breadth is washed out but recovering, BTC is increasing, and the symbol trend is positive
            - Score: {trigger_score:.2f}
            - Volume: {round_numbers(float(row.volume), decimals=self.price_precision)} {base_asset}
            - OI Growth: {self.oi_growth:.2f}
            - Market breadth: {round_numbers(self._latest_market_breadth(context), 3) if context else "UNAVAILABLE"}
            - BTC momentum: {round_numbers(float(btc_momentum), 5)}
            - Market regime: {context.market_regime if context and context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context and context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Autotrade route: {route_reason}
            - Market stress: {round_numbers(context.market_stress_score, 3) if context else 0}
            - Exit profile: fixed {self.LONG_STOP_LOSS_PCT}% stop / {self.LONG_TAKE_PROFIT_PCT}% take profit / 8-candle maximum hold
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        self.ti.dispatch_signal_record(value=value)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

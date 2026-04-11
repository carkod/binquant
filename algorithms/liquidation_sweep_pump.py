from os import getenv
from typing import TYPE_CHECKING
from pybinbot import (
    HABollinguerSpread,
    Strategy,
    round_numbers,
    KlineSchema,
    MarketType,
    SignalsConsumer,
)
from market_regime_prediction.score_signal_candidate_with_context import (
    score_signal_candidate_with_context,
)
from market_regime_prediction.signal_context_scorer import SignalContextScorer
from pandera.typing import DataFrame as TypedDataFrame
from consumers.signal_collector import SignalCollector
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class LiquidationSweepPump:
    def __init__(self, cls: "ContextEvaluator"):
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
        self.df: TypedDataFrame[KlineSchema] = cls.df_15m.copy()
        self.df_btc: TypedDataFrame[KlineSchema] = cls.df_btc.copy()
        self.latest_market_context = cls.latest_market_context
        self.signal_context_scorer = SignalContextScorer(
            context_weight=0.12,
            risk_weight=0.1,
            support_weight=0.08,
        )
        self.signal_collector = SignalCollector(
            first_seen_at=cls.first_seen_at,
            interval=cls.interval,
            binbot_api=cls.binbot_api,
        )

    def compute_pump_score(self, window_hours=3) -> TypedDataFrame[KlineSchema]:
        """
        Compute pump score using:
        - Relative volume
        - Early momentum
        - Price compression
        - OI growth (cached per asset; defaults to 1.0 if unavailable)
        """
        df = self.df.copy()

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
        if self.df is None or self.df.empty:
            return None

        algo = "liquidation_sweep_pump"
        bot_strategy = Strategy.long
        autotrade = True
        base_asset = self.current_symbol_data["base_asset"]

        df = self.compute_pump_score()

        # --- Filters ---
        # Take last N candles (say 48 for 12h)
        recent_scores = df["pump_score_smooth"].iloc[-48:]
        btc_momentum = self.df_btc.close.pct_change().iloc[-1]

        # Compute dynamic threshold
        # For example, top 5% of historical scores
        PUMP_SCORE_THRESHOLD = recent_scores.quantile(0.95)
        row = df.iloc[-1]
        latest_score = row["pump_score_smooth"]

        if btc_momentum < -0.01:
            return

        if latest_score is None or float(latest_score) < PUMP_SCORE_THRESHOLD:
            return

        # Optional OI confirmation
        if self.oi_growth is not None and self.oi_growth < 1.05:
            return

        local_score = float(latest_score / PUMP_SCORE_THRESHOLD)
        symbol_return = float(df["close"].pct_change().iloc[-1]) if len(df) > 1 else 0.0
        btc_return = (
            float(self.df_btc["close"].pct_change().iloc[-1])
            if not self.df_btc.empty and len(self.df_btc) > 1
            else 0.0
        )
        ema_fast = df["close"].ewm(span=9, adjust=False).mean().iloc[-1]
        ema_slow = df["close"].ewm(span=21, adjust=False).mean().iloc[-1]
        trend_score = (
            float((ema_fast - ema_slow) / abs(ema_slow))
            if float(ema_slow) != 0
            else 0.0
        )

        kucoin_link, terminal_link = build_links_msg(
            self.config.env, self.exchange, self.market_type, self.symbol
        )

        msg = f"""
            - [{getenv("ENV")}] <strong>#{algo}</strong> #{self.symbol}
            - Current price: {round_numbers(current_price, decimals=self.price_precision)}
            - Score: {latest_score:.2f}
            - 📊 {base_asset} volume: {round_numbers(float(row.volume), decimals=self.price_precision)}
            - OI Growth: {self.oi_growth:.2f}
            - Autotrade?: {"Yes" if autotrade else "No"}
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """

        candidate = SignalsConsumer(
            symbol=self.symbol,
            algo=algo,
            direction="LONG",
            msg=msg,
            strategy=bot_strategy,
            autotrade=autotrade,
            market_type=MarketType.FUTURES,
            score=local_score,
            current_price=current_price,
            volume=float(row.volume),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        evaluation = score_signal_candidate_with_context(
            candidate=candidate,
            market_context=self.latest_market_context,
            scorer=self.signal_context_scorer,
            local_features={
                "relative_strength_vs_btc": symbol_return - btc_return,
                "trend_score": trend_score,
            },
            emit_threshold=1.0,
        )
        context_score = evaluation.context_score
        if self.latest_market_context is not None:
            if self.latest_market_context.market_stress_score >= 0.35:
                autotrade = False
            elif self.latest_market_context.advancers_ratio >= 0.55:
                autotrade = bot_strategy == Strategy.long
            elif self.latest_market_context.advancers_ratio <= 0.45:
                # liquidation sweep pump is mostly designed as a long bot
                return

            in_long_regime = (
                self.latest_market_context.advancers_ratio >= 0.55
                and self.latest_market_context.long_tailwind > 0
            )
            in_neutral_transition = (
                0.45 < self.latest_market_context.advancers_ratio < 0.55
            )
            high_market_stress = self.latest_market_context.market_stress_score >= 0.35

            if not in_long_regime or in_neutral_transition or high_market_stress:
                return
        if (
            context_score.confidence >= 0.65
            and context_score.followthrough_score < -0.35
            and context_score.adverse_excursion_risk > 0.75
        ):
            return
        if not evaluation.emit:
            return

        msg += f"""
            - Context confidence: {round_numbers(context_score.confidence, 2)}
            - Long regime: {"Yes" if self.latest_market_context and self.latest_market_context.advancers_ratio >= 0.55 and self.latest_market_context.long_tailwind > 0 else "No"}
            - Follow-through: {round_numbers(context_score.followthrough_score, 3)}
            - Risk: {round_numbers(context_score.adverse_excursion_risk, 3)}
            - Market stress: {round_numbers(self.latest_market_context.market_stress_score, 3) if self.latest_market_context else 0}
            - Adjusted score: {round_numbers(evaluation.adjusted_score, 3)}
        """

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(evaluation.candidate)

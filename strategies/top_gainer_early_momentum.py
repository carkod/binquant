import logging
from datetime import UTC, datetime
from math import isfinite
from os import getenv
from typing import TYPE_CHECKING

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
from shared.utils import build_links_msg, format_context_timestamp_line

if TYPE_CHECKING:
    from pandas import DataFrame

    from producers.context_evaluator import ContextEvaluator


class TopGainerEarlyMomentum:
    """
    Long-only continuation setup for assets entering the top-gainer tape.

    The production top-gainer sample showed the first tradable impulse usually
    shared four traits: close above the recent 15m high, accelerating 1h/2h
    return, volume expansion, and a candle closing near its high. This strategy
    waits for two closes to confirm that ignition while rejecting
    already-vertical blow-offs.

    Conservative version. Do not make changes unless we are sure
    the strategy will perform better
    """

    ALGO = "top_gainer_early_momentum"

    LOOKBACK_HIGH_WINDOW = 48
    VOLUME_WINDOW = 32
    MIN_HISTORY = 56

    MIN_CANDLE_RETURN = 0.01
    MAX_CANDLE_RETURN = 0.10
    MIN_RETURN_1H = 0.05
    MIN_RETURN_2H = 0.08
    MIN_RETURN_6H = 0.07
    MAX_RETURN_1H = 0.20
    FULL_EXTENSION_WINDOW_BARS = 96
    MAX_EXTENSION_RETURN = 0.50
    MIN_SHORT_HISTORY_EXTENSION_CAP = 0.25
    MIN_VOLUME_RATIO = 1.75
    MIN_CLOSE_RANGE_POSITION = 0.75
    MAX_UPPER_WICK_FRACTION = 0.30

    MAX_MARKET_STRESS_SCORE = 0.25
    MIN_RELATIVE_STRENGTH_VS_BTC = 0.03
    MAX_SYMBOL_ATR_PCT = 0.06

    FIAT_ORDER_SIZE_FRACTION = 1 / 3
    ATR_STOP_MULT = 2.2
    MIN_STOP_LOSS_PCT = 1.5
    MAX_STOP_LOSS_PCT = 2.0
    ENTRY_COOLDOWN_MINUTES = 60
    TRAILING_PROFIT_PCT = 3.0
    TRAILING_DEVIATION_PCT = 1.5
    RISK_REJECTION_ALGO = f"{ALGO}:risk_rejection"

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
        self._last_risk_rejection_candle: int | None = None

    def _fiat_order_size(self) -> float:
        settings = getattr(self.at_consumer, "autotrade_settings", None)
        base_order_size = float(getattr(settings, "base_order_size", 0.0) or 0.0)
        return round_numbers(base_order_size * self.FIAT_ORDER_SIZE_FRACTION, 8)

    @classmethod
    def _features(cls, df: "DataFrame") -> tuple[dict[str, float] | None, str]:
        if len(df) < cls.MIN_HISTORY:
            return None, "history_too_short"
        for column in ("open", "high", "low", "close", "volume", "open_time"):
            if column not in df.columns:
                return None, f"{column}_column_unavailable"

        candidate = df.iloc[-1]
        close = float(candidate["close"])
        open_ = float(candidate["open"])
        high = float(candidate["high"])
        low = float(candidate["low"])
        volume = float(candidate["volume"])
        if min(close, open_, high, low, volume) <= 0:
            return None, "invalid_candle_values"

        previous_high = float(df["high"].iloc[-cls.LOOKBACK_HIGH_WINDOW - 1 : -1].max())
        volume_ma = float(df["volume"].rolling(cls.VOLUME_WINDOW).mean().iloc[-1])
        quote_volume = (
            float(candidate["quote_asset_volume"])
            if "quote_asset_volume" in df.columns
            else volume * close
        )
        quote_volume_ma = (
            float(df["quote_asset_volume"].rolling(cls.VOLUME_WINDOW).mean().iloc[-1])
            if "quote_asset_volume" in df.columns
            else volume_ma * close
        )
        atr = float(candidate["ATR"]) if "ATR" in df.columns else 0.0

        range_size = high - low
        upper_wick = high - max(open_, close)
        extension_anchor_index = max(0, len(df) - cls.FULL_EXTENSION_WINDOW_BARS - 1)
        extension_window_bars = len(df) - extension_anchor_index - 1
        extension_cap = cls.MAX_EXTENSION_RETURN
        if extension_window_bars < cls.FULL_EXTENSION_WINDOW_BARS:
            extension_cap = max(
                cls.MIN_SHORT_HISTORY_EXTENSION_CAP,
                cls.MAX_EXTENSION_RETURN
                * (extension_window_bars / cls.FULL_EXTENSION_WINDOW_BARS),
            )
        values = {
            "close": close,
            "open": open_,
            "high": high,
            "low": low,
            "volume": volume,
            "quote_volume": quote_volume,
            "previous_high": previous_high,
            "return_1h": close / float(df["close"].iloc[-5]) - 1,
            "return_2h": close / float(df["close"].iloc[-9]) - 1,
            "return_6h": close / float(df["close"].iloc[-25]) - 1,
            "extension_return": close / float(df["close"].iloc[extension_anchor_index])
            - 1,
            "extension_window_bars": float(extension_window_bars),
            "extension_cap": extension_cap,
            "candle_return": close / open_ - 1,
            "volume_ratio": volume / (volume_ma + 1e-6),
            "quote_volume_ratio": quote_volume / (quote_volume_ma + 1e-6),
            "range_position": (close - low) / (range_size + 1e-6),
            "upper_wick_fraction": upper_wick / (range_size + 1e-6),
            "ema20": float(df["close"].ewm(span=20, adjust=False).mean().iloc[-1]),
            "ema50": float(df["close"].ewm(span=50, adjust=False).mean().iloc[-1]),
            "atr": atr,
        }
        if not all(isfinite(value) for value in values.values()):
            return None, "indicators_not_ready"
        return values, "features_ready"

    @classmethod
    def _entry_allows(cls, values: dict[str, float]) -> tuple[bool, str]:
        if values["close"] <= values["previous_high"]:
            return False, "not_breaking_recent_high"
        if values["close"] <= values["ema20"] or values["close"] <= values["ema50"]:
            return False, "not_above_ema20_and_ema50"
        if values["candle_return"] < cls.MIN_CANDLE_RETURN:
            return False, "candle_not_green_enough"
        if values["candle_return"] > cls.MAX_CANDLE_RETURN:
            return False, "single_candle_too_extended"
        if values["return_1h"] > cls.MAX_RETURN_1H:
            return False, "one_hour_move_too_extended"
        if values["extension_return"] > values["extension_cap"]:
            return False, "extension_move_too_extended"
        if (
            values["return_1h"] < cls.MIN_RETURN_1H
            and values["return_2h"] < cls.MIN_RETURN_2H
        ):
            return False, "momentum_not_accelerating"
        if values["return_6h"] < cls.MIN_RETURN_6H:
            return False, "six_hour_move_not_confirmed"
        if values["volume_ratio"] < cls.MIN_VOLUME_RATIO:
            return False, "volume_ratio_too_low"
        if values["range_position"] < cls.MIN_CLOSE_RANGE_POSITION:
            return False, "close_not_near_high"
        if values["upper_wick_fraction"] > cls.MAX_UPPER_WICK_FRACTION:
            return False, "upper_wick_too_large"
        return True, "top_gainer_breakout_ignition"

    @staticmethod
    def _completed_candles(df: "DataFrame", now_ms: int) -> "DataFrame":
        if "close_time" not in df.columns:
            return df.iloc[0:0]
        return df.loc[df["close_time"] < now_ms]

    @classmethod
    def _confirmation_allows(
        cls,
        *,
        breakout_close: float,
        previous_high: float,
        first_confirmation_close: float,
        second_confirmation_open: float,
        second_confirmation_high: float,
        second_confirmation_low: float,
        second_confirmation_close: float,
    ) -> tuple[bool, str]:
        if first_confirmation_close <= previous_high:
            return False, "first_confirmation_did_not_hold_breakout"
        if second_confirmation_close <= breakout_close:
            return False, "second_confirmation_did_not_clear_breakout_close"
        if second_confirmation_close <= first_confirmation_close:
            return False, "second_confirmation_did_not_retain_momentum"

        confirmation_range = second_confirmation_high - second_confirmation_low
        if confirmation_range <= 0:
            return False, "second_confirmation_range_invalid"
        range_position = (
            second_confirmation_close - second_confirmation_low
        ) / confirmation_range
        upper_wick_fraction = (
            second_confirmation_high
            - max(second_confirmation_open, second_confirmation_close)
        ) / confirmation_range
        if range_position < cls.MIN_CLOSE_RANGE_POSITION:
            return False, "second_confirmation_close_not_near_high"
        if upper_wick_fraction > cls.MAX_UPPER_WICK_FRACTION:
            return False, "second_confirmation_upper_wick_too_large"
        return True, "top_gainer_breakout_two_close_confirmation"

    @classmethod
    def _risk_profile_allows(
        cls,
        *,
        context: LiveMarketContext | None,
        features: SymbolMarketFeatures | None,
    ) -> tuple[bool, str]:
        if context is None:
            return False, "market_context_unavailable"
        if context.market_stress_score >= cls.MAX_MARKET_STRESS_SCORE:
            return False, "market_stress_too_high"
        if context.short_regime_score > context.long_regime_score + 0.15:
            return False, "market_context_short_edge"
        if context.btc_regime_score < -0.2 and context.btc_return < 0:
            return False, "btc_context_down"

        if features is None:
            return False, "symbol_regime_unavailable"
        if features.relative_strength_vs_btc <= cls.MIN_RELATIVE_STRENGTH_VS_BTC:
            return False, "relative_strength_vs_btc_not_positive"
        if features.atr_pct > cls.MAX_SYMBOL_ATR_PCT:
            return False, "symbol_atr_too_high"
        if features.micro_regime_transition in {
            "BREAKDOWN",
            "ENTERED_TREND_DOWN",
            "VOLATILITY_EXPANSION",
        }:
            return False, "symbol_transition_not_long"
        if features.micro_regime == "TREND_DOWN" and features.trend_score < 0:
            return False, "symbol_trend_down"
        return True, "risk_profile_allows_long"

    def _stop_loss_pct(self, close: float, atr: float) -> float:
        if close <= 0 or atr <= 0:
            return self.MIN_STOP_LOSS_PCT
        pct = (self.ATR_STOP_MULT * atr / close) * 100
        return round_numbers(
            min(max(pct, self.MIN_STOP_LOSS_PCT), self.MAX_STOP_LOSS_PCT),
            4,
        )

    @staticmethod
    def _score(values: dict[str, float]) -> float:
        return round_numbers(
            1.0
            + min(values["return_1h"] * 4, 0.8)
            + min(values["volume_ratio"] / 10, 0.5)
            + min(values["range_position"] / 4, 0.25),
            4,
        )

    def _already_emitted(self, candle_open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_emitted_candle == candle_open_time
        return self.strategy_cooldowns.get((self.ALGO, self.symbol)) == candle_open_time

    def _mark_emitted(self, candle_open_time: int) -> None:
        self._last_emitted_candle = candle_open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.ALGO, self.symbol)] = candle_open_time

    def _risk_rejection_already_recorded(self, candle_open_time: int) -> bool:
        if self.strategy_cooldowns is None:
            return self._last_risk_rejection_candle == candle_open_time
        return (
            self.strategy_cooldowns.get((self.RISK_REJECTION_ALGO, self.symbol))
            == candle_open_time
        )

    def _record_risk_rejection(
        self,
        *,
        candle_open_time: int,
        current_price: float,
        context: LiveMarketContext | None,
        features: SymbolMarketFeatures | None,
        values: dict[str, float],
        risk_reason: str,
    ) -> None:
        if self._risk_rejection_already_recorded(candle_open_time):
            return

        self._last_risk_rejection_candle = candle_open_time
        if self.strategy_cooldowns is not None:
            self.strategy_cooldowns[(self.RISK_REJECTION_ALGO, self.symbol)] = (
                candle_open_time
            )

        indicators = {
            "observation": "risk_rejection",
            "risk_reason": risk_reason,
            "candidate_open_time": candle_open_time,
            "current_price": current_price,
            "market_regime": context.market_regime if context else None,
            "market_regime_transition": (
                context.market_regime_transition if context else None
            ),
            "market_stress_score": context.market_stress_score if context else None,
            "btc_return": context.btc_return if context else None,
            "btc_regime_score": context.btc_regime_score if context else None,
            "symbol_micro_regime": features.micro_regime if features else None,
            "symbol_micro_regime_transition": (
                features.micro_regime_transition if features else None
            ),
            "symbol_trend_score": features.trend_score if features else None,
            "symbol_atr_pct": features.atr_pct if features else None,
            "relative_strength_vs_btc": (
                features.relative_strength_vs_btc if features else None
            ),
            "return_1h": values["return_1h"],
            "return_2h": values["return_2h"],
            "return_6h": values["return_6h"],
            "volume_ratio": values["volume_ratio"],
        }
        logging.info(
            "%s risk rejected: symbol=%s reason=%s market_regime=%s "
            "market_transition=%s symbol_regime=%s symbol_transition=%s "
            "atr_pct=%s relative_strength_vs_btc=%s",
            self.ALGO,
            self.symbol,
            risk_reason,
            indicators["market_regime"],
            indicators["market_regime_transition"],
            indicators["symbol_micro_regime"],
            indicators["symbol_micro_regime_transition"],
            indicators["symbol_atr_pct"],
            indicators["relative_strength_vs_btc"],
        )
        try:
            self.ti.binbot_api.dispatch_create_signal(
                algorithm_name=self.ALGO,
                symbol=self.symbol,
                generated_at=datetime.now(UTC),
                direction=Position.long.value,
                autotrade=False,
                current_regime=context.market_regime if context else None,
                context=context.model_dump(mode="json") if context else {},
                signal_kind="risk_rejection",
                bot_params={},
                grid_params={},
                indicators=indicators,
            )
        except Exception:
            logging.exception(
                "%s risk-rejection persistence failed for %s",
                self.ALGO,
                self.symbol,
            )

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        if self.market_type != MarketType.FUTURES:
            return

        df = self._completed_candles(
            self.ti.df_15m,
            now_ms=int(datetime.now(UTC).timestamp() * 1000),
        )
        if len(df) < self.MIN_HISTORY + 2:
            logging.info("%s skipped: history_too_short", self.ALGO)
            return

        breakout_df = df.iloc[:-2]
        values, feature_reason = self._features(breakout_df)
        if values is None:
            logging.info("%s skipped: %s", self.ALGO, feature_reason)
            return

        entry_allowed, entry_reason = self._entry_allows(values)
        if not entry_allowed:
            logging.info("%s skipped: %s", self.ALGO, entry_reason)
            return

        first_confirmation = df.iloc[-2]
        candidate = df.iloc[-1]
        confirmation_allowed, confirmation_reason = self._confirmation_allows(
            breakout_close=values["close"],
            previous_high=values["previous_high"],
            first_confirmation_close=float(first_confirmation["close"]),
            second_confirmation_open=float(candidate["open"]),
            second_confirmation_high=float(candidate["high"]),
            second_confirmation_low=float(candidate["low"]),
            second_confirmation_close=float(candidate["close"]),
        )
        if not confirmation_allowed:
            logging.info("%s skipped: %s", self.ALGO, confirmation_reason)
            return

        context = self.ti.latest_market_context
        symbol_features = resolve_symbol_features(context=context, symbol=self.symbol)
        risk_allowed, risk_reason = self._risk_profile_allows(
            context=context,
            features=symbol_features,
        )
        if not risk_allowed:
            self._record_risk_rejection(
                candle_open_time=int(candidate["open_time"]),
                current_price=current_price,
                context=context,
                features=symbol_features,
                values=values,
                risk_reason=risk_reason,
            )
            return
        assert context is not None

        candidate_open_time = int(candidate["open_time"])
        if self._already_emitted(candidate_open_time):
            logging.info("%s skipped: candle_already_emitted", self.ALGO)
            return
        self._mark_emitted(candidate_open_time)

        autotrade = True
        route_reason = "confirmed_top_gainer_long"
        fiat_order_size = self._fiat_order_size()
        stop_loss = self._stop_loss_pct(
            close=float(candidate["close"]),
            atr=float(candidate["ATR"]) if "ATR" in df.columns else 0.0,
        )
        score = self._score(values)
        base_asset = self.current_symbol_data.base_asset
        quote_asset = self.current_symbol_data.quote_asset
        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )

        indicators = {
            **values,
            "breakout_reason": entry_reason,
            "entry_reason": confirmation_reason,
            "breakout_open_time": int(breakout_df.iloc[-1]["open_time"]),
            "first_confirmation_close": float(first_confirmation["close"]),
            "second_confirmation_close": float(candidate["close"]),
            "risk_reason": risk_reason,
            "route_reason": route_reason,
            "stop_loss_pct": stop_loss,
            "entry_cooldown_minutes": self.ENTRY_COOLDOWN_MINUTES,
            "trailing_profit_pct": self.TRAILING_PROFIT_PCT,
            "trailing_deviation_pct": self.TRAILING_DEVIATION_PCT,
        }

        value = SignalsConsumer(
            direction=Position.long.value.upper(),
            autotrade=autotrade,
            current_price=float(current_price),
            volume=values["volume"],
            score=score,
            bot_params=BotBase(
                pair=self.symbol,
                name=self.ALGO,
                position=Position.long,
                market_type=MarketType.FUTURES,
                cooldown=self.ENTRY_COOLDOWN_MINUTES,
                dynamic_trailing=True,
                fiat_order_size=fiat_order_size,
                stop_loss=stop_loss,
                trailing=True,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                trailing_profit=self.TRAILING_PROFIT_PCT,
                margin_short_reversal=False,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )
        self.ti.finalize_signal_bot_params(value)
        assert value.bot_params is not None
        fiat_order_size = value.bot_params.fiat_order_size

        msg = f"""
            - [{getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
            - Action: LONG ENTRY
            - Current price: {round_numbers(float(current_price), decimals=self.price_precision)}
            - Rule intent: BUY confirmed top-gainer breakouts after price holds the recent high and then clears the breakout close
            - Breakout setup: {entry_reason}
            - Entry setup: {confirmation_reason}
            - Breakout / first confirmation / entry close: {round_numbers(values["close"], self.price_precision)} / {round_numbers(float(first_confirmation["close"]), self.price_precision)} / {round_numbers(float(candidate["close"]), self.price_precision)}
            - 1h / 2h / 6h / extension return ({int(values["extension_window_bars"])} bars, cap {round_numbers(values["extension_cap"] * 100, 2)}%): {round_numbers(values["return_1h"] * 100, 2)}% / {round_numbers(values["return_2h"] * 100, 2)}% / {round_numbers(values["return_6h"] * 100, 2)}% / {round_numbers(values["extension_return"] * 100, 2)}%
            - Candle return: {round_numbers(values["candle_return"] * 100, 2)}%
            - Volume: {round_numbers(values["volume"], decimals=self.price_precision)} {base_asset} (ratio {round_numbers(values["volume_ratio"], 2)})
            - Quote volume: {round_numbers(values["quote_volume"], decimals=self.price_precision)} {quote_asset} (ratio {round_numbers(values["quote_volume_ratio"], 2)})
            - Market regime: {context.market_regime if context.market_regime is not None else "UNAVAILABLE"}
            - Market transition: {context.market_regime_transition if context.market_regime_transition is not None else "None"}
            {format_context_timestamp_line(context)}
            - Coin regime: {symbol_features.micro_regime if symbol_features and symbol_features.micro_regime is not None else "UNAVAILABLE"}
            - Coin transition: {symbol_features.micro_regime_transition if symbol_features and symbol_features.micro_regime_transition is not None else "None"}
            - Autotrade route: {route_reason}
            - Max margin: {fiat_order_size} {quote_asset}
            - Stop loss: {stop_loss}%
            - Trailing profit / deviation: {self.TRAILING_PROFIT_PCT}% / {self.TRAILING_DEVIATION_PCT}%
            - Pair cooldown: {self.ENTRY_COOLDOWN_MINUTES} minutes
            - Confidence score: {score}
            - Signal timestamp: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}
            - Autotrade is enabled
            - <a href='{kucoin_link}'>KuCoin</a>
            - <a href='{terminal_link}'>Dashboard trade</a>
        """
        self.ti.dispatch_signal_record(value=value, indicators=indicators)
        self.telegram_consumer.dispatch_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

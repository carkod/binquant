import logging
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pybinbot import HABollinguerSpread, Position, SignalsConsumer, round_numbers

from market_regime.regime_routing import allows_long_autotrade
from shared.utils import build_links_msg

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


class CoinruleBuyTheDip:
    ALGO = "coinrule_buy_the_dip"
    START_TIME = datetime(2026, 4, 12, 23, 21, tzinfo=UTC)
    LOOKBACK_HOURS = 6
    LOOKBACK_CANDLES = 24
    BUY_SIZE_USDT = 15.0

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

    @staticmethod
    def pct_change(current: float, past: float) -> float:
        if past <= 0:
            return 0.0
        return ((current - past) / past) * 100.0

    @staticmethod
    def _normalize_timestamp(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)

        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            return None

        return datetime.fromtimestamp(timestamp / 1000, tz=UTC)

    def _find_reference_price(self, target_time: datetime) -> float | None:
        if "close_time" not in self.df_15m.columns:
            return None

        for _, candle in self.df_15m.iloc[::-1].iterrows():
            candle_time = self._normalize_timestamp(candle.get("close_time"))
            if candle_time is None:
                continue
            if candle_time <= target_time:
                return float(candle["close"])
        return None

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
    ) -> None:
        self.df_15m = self.ti.df_15m.copy()
        required_cols = ["close", "close_time"]

        if (
            len(self.df_15m) < self.LOOKBACK_CANDLES
            or self.df_15m[required_cols].isnull().any().any()
        ):
            logging.info(
                "Buy-the-dip skipped: not enough 15m data for symbol %s",
                self.symbol,
            )
            return

        now = self._normalize_timestamp(self.df_15m["close_time"].iloc[-1])
        if now is None:
            logging.info("Buy-the-dip skipped: invalid close_time for %s", self.symbol)
            return

        if now < self.START_TIME:
            return

        current_price = float(current_price)
        reference_price = self._find_reference_price(
            target_time=now - timedelta(hours=self.LOOKBACK_HOURS)
        )
        if reference_price is None:
            return

        change_6h = self.pct_change(current_price, reference_price)
        if change_6h > -2.0 or change_6h <= -5.0:
            return

        kucoin_link, terminal_link = build_links_msg(
            self.config.env,
            self.exchange,
            self.market_type,
            self.symbol,
        )
        context = self.latest_market_context
        autotrade = (
            allows_long_autotrade(context=context, symbol=self.symbol)
            if context is not None
            else False
        )

        msg = f"""
        - [{os.getenv("ENV")}] <strong>#{self.ALGO} algorithm</strong> #{self.symbol}
        - Action: LONG ENTRY
        - Current price: {round_numbers(current_price, 6)}
        - Strategy: {Position.long.value}
        - Rule intent: BUY ${self.BUY_SIZE_USDT:.2f} after a 6h dip between -2.0% and -5.0%
        - 6h reference price: {round_numbers(reference_price, 6)}
        - 6h price change: {round_numbers(change_6h, 2)}%
        - Candle time: {now.isoformat()}
        - {"Autotrade is enabled" if autotrade else "Autotrade is disabled"}
        - <a href='{kucoin_link}'>KuCoin</a>
        - <a href='{terminal_link}'>Dashboard trade</a>
        """

        value = SignalsConsumer(
            autotrade=autotrade,
            current_price=current_price,
            symbol=self.symbol,
            algo=self.ALGO,
            bot_strategy=Position.long,
            market_type=self.market_type,
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high,
                bb_mid=bb_mid,
                bb_low=bb_low,
            ),
        )

        await self.telegram_consumer.send_signal(msg)
        await self.at_consumer.process_autotrade_restrictions(value)

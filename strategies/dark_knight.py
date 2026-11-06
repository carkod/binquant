"""Dark Knight: an independent, observation-only short/leverage experiment."""

import logging
from dataclasses import asdict
from decimal import ROUND_CEILING, Decimal
from math import isfinite
from time import time
from typing import TYPE_CHECKING, Any

from pandas import DataFrame, concat, to_numeric
from pybinbot import (
    BotBase,
    HABollinguerSpread,
    MarketType,
    Position,
    SignalsConsumer,
    timestamp_sort_key,
)
from pybinbot.shared.price_crossings import (
    CHOP_CROSSING_THRESHOLD,
    price_crossings_six_hours,
)

from shared.higher_low import confirmed_higher_low
from strategies.lower_high_pattern import LowerHighPattern

if TYPE_CHECKING:
    from producers.context_evaluator import ContextEvaluator


SCORING_VERSION = "dark_knight_v2"
# Lower bound, proposed leverage, descriptive quality tier.
LEVERAGE_TIERS = (
    (80, 5, "exceptional"),
    (65, 4, "strong"),
    (50, 3, "moderate"),
    (35, 2, "developing"),
    (0, 1, "weak"),
)


def leverage_tier(score: float) -> tuple[int, str]:
    if not isfinite(score):
        raise ValueError("Quality score must be finite")
    for threshold, leverage, name in LEVERAGE_TIERS:
        if score >= threshold:
            return leverage, name
    return 1, "weak"


def completed_ohlcv(
    df: DataFrame | None, *, now_ms: float, interval_ms: int, bars: int
) -> DataFrame | None:
    """Reject incomplete, stale or malformed history; ignore forming candles."""
    columns = ["open_time", "close_time", "open", "high", "low", "close", "volume"]
    if df is None or not set(columns).issubset(df.columns):
        return None
    frame = df[columns].apply(to_numeric, errors="coerce")
    if not frame[["open_time", "close_time"]].map(isfinite).all().all():
        return None
    frame = frame.loc[frame.close_time < now_ms].sort_values("open_time").tail(bars)
    if len(frame) < bars or not frame.map(isfinite).all().all():
        return None
    if not 0 <= now_ms - frame.close_time.iloc[-1] <= interval_ms:
        return None
    if not (frame.open_time.diff().iloc[1:] == interval_ms).all():
        return None
    duration = frame.close_time - frame.open_time
    if not duration.between(interval_ms - 1, interval_ms).all():
        return None
    if (
        (frame[["open", "high", "low", "close"]] <= 0).any().any()
        or (frame.volume < 0).any()
        or (frame.high < frame[["open", "close", "low"]].max(axis=1)).any()
        or (frame.low > frame[["open", "close", "high"]].min(axis=1)).any()
    ):
        return None
    return frame.reset_index(drop=True)


def swing_positions(values: list[float], *, high: bool) -> list[int]:
    """Strict two-wing fractals; the final two candles cannot be pivots."""
    return [
        i
        for i in range(2, len(values) - 2)
        if all(
            (values[i] > values[j] if high else values[i] < values[j])
            for j in range(i - 2, i + 3)
            if i != j
        )
    ]


def breadth_crossings_six_hours(
    samples: list[tuple[float, float]], *, now_seconds: float
) -> int | None:
    """Count side changes around the latest breadth value over 24 prior samples."""
    if len(samples) < 25:
        return None
    window = sorted(samples, key=lambda sample: sample[0])[-25:]
    latest_timestamp, current_value = window[-1]
    if (
        not isfinite(current_value)
        or not 0 <= now_seconds - latest_timestamp <= 30 * 60
        or not 5 * 3600 + 45 * 60
        <= latest_timestamp - window[0][0]
        <= 6 * 3600 + 15 * 60
        or any(not isfinite(stamp) or not isfinite(value) for stamp, value in window)
        or any(
            not 0 < later[0] - earlier[0] <= 30 * 60
            for earlier, later in zip(window, window[1:], strict=False)
        )
    ):
        return None

    previous_side = 0
    crossings = 0
    for _, value in window[:-1]:
        side = 1 if value > current_value else -1 if value < current_value else 0
        if previous_side and side and side != previous_side:
            crossings += 1
        if side:
            previous_side = side
    return crossings


def score_short_setup(
    frame: DataFrame,
    *,
    current_price: float,
    earlier_peak: int,
    later_peak: int,
    btc: DataFrame | None,
    hourly: DataFrame | None,
    daily: DataFrame | None,
    micro_direction: str | None,
    crossings: int,
) -> dict[str, Any]:
    """Score evidence, not probability; unknown optional evidence earns no points.

    Frames must be validated, completed OHLCV. Peak positions refer to frame.
    All thresholds are provisional and versioned for subsequent calibration.
    """
    previous_close = frame.close.shift(1)
    true_range = concat(
        [
            frame.high - frame.low,
            (frame.high - previous_close).abs(),
            (frame.low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    # A simple 14-bar mean of true range, recomputed from completed candles.
    atr = float(true_range.tail(14).mean())
    if not isfinite(atr) or atr <= 0:
        raise ValueError("ATR must be positive")
    first_high = float(frame.high.iloc[earlier_peak])
    second_high = float(frame.high.iloc[later_peak])
    support = float(frame.low.iloc[earlier_peak + 1 : later_peak].min())
    after_peak = frame.iloc[later_peak + 1 :]
    breakdowns = after_peak.close < support - 0.25 * atr
    below_support = bool(breakdowns.iloc[-1]) and current_price < support
    held_breakdown = (
        below_support and len(breakdowns) >= 2 and bool(breakdowns.iloc[-2:].all())
    )
    separation_atr = (first_high - second_high) / atr
    structure = (
        10 * min(1.0, separation_atr / 2) + 10 * below_support + 10 * held_breakdown
    )

    latest = frame.iloc[-1]
    candle_range = float(latest.high - latest.low)
    bearish_close = bool(latest.close < latest.open)
    close_location = (
        float((latest.high - latest.close) / candle_range) if candle_range else 0.0
    )
    body_atr = float((latest.open - latest.close) / atr)
    baseline_volume = float(frame.volume.iloc[-21:-1].median())
    volume_ratio = (
        float(latest.volume / baseline_volume) if baseline_volume > 0 else None
    )
    selling = 10 * min(1.0, max(0.0, body_atr)) * close_location + (
        10 * min(1.0, max(0.0, volume_ratio - 1))
        if bearish_close and volume_ratio is not None
        else 0
    )

    relative = 0.0
    coin_return = btc_return = underperformance_atr = None
    if btc is not None:
        aligned = (
            frame[["open_time", "close"]]
            .merge(
                btc[["open_time", "close"]], on="open_time", suffixes=("_coin", "_btc")
            )
            .tail(5)
        )
        if (
            len(aligned) == 5
            and (aligned.open_time.diff().iloc[1:] == 900_000).all()
            and aligned.open_time.iloc[-1] == frame.open_time.iloc[-1]
        ):
            coin_return = float(
                aligned.close_coin.iloc[-1] / aligned.close_coin.iloc[0] - 1
            )
            btc_return = float(
                aligned.close_btc.iloc[-1] / aligned.close_btc.iloc[0] - 1
            )
            underperformance_atr = (btc_return - coin_return) / (
                atr / float(latest.close)
            )
            # A BTC higher low alone is not bearish evidence for this coin.
            if btc_return > 0 and coin_return < 0:
                relative = 20 * min(1.0, max(0.0, underperformance_atr) / 2)

    hourly_bearish = False
    support_candidates: list[float] = []
    for history in (frame, hourly):
        if history is not None:
            lows = [float(value) for value in history.low]
            support_candidates.extend(
                lows[i]
                for i in swing_positions(lows, high=False)
                if lows[i] < current_price
            )
    if hourly is not None:
        highs = [float(value) for value in hourly.high]
        lows = [float(value) for value in hourly.low]
        high_pivots = swing_positions(highs, high=True)
        low_pivots = swing_positions(lows, high=False)
        hourly_bearish = (
            len(high_pivots) >= 2
            and len(low_pivots) >= 2
            and highs[high_pivots[-1]] < highs[high_pivots[-2]]
            and lows[low_pivots[-1]] < lows[low_pivots[-2]]
        )
    timeframe = 10 * hourly_bearish + 5 * (micro_direction == "DOWN")

    # Daily candles provide the extension evidence.
    extension = 0.0
    daily_extension = None
    if daily is not None:
        # The last 7 days are excluded from the historical baseline.
        baseline = float(daily.close.iloc[:-7].median())
        daily_extension = (second_high / baseline - 1) * 100
        extension += 5 * min(1.0, max(0.0, daily_extension) / 100)

    late_entry_atr = max(0.0, (support - current_price) / atr)
    chop_penalty = crossings * 5.0
    late_penalty = min(15.0, max(0.0, late_entry_atr - 2) * 5)
    components = {
        "structure": round(structure, 4),
        "selling_pressure": round(selling, 4),
        "relative_weakness": round(relative, 4),
        "timeframe_agreement": float(timeframe),
        "prior_extension": round(extension, 4),
    }
    score = round(
        max(0.0, min(100.0, sum(components.values()) - chop_penalty - late_penalty)), 4
    )
    leverage, tier = leverage_tier(score)
    next_support = max(support_candidates) if support_candidates else None
    downside = (
        (current_price - next_support) / current_price * 100
        if next_support is not None
        else None
    )
    return {
        "scoring_version": SCORING_VERSION,
        "quality_score": score,
        "quality_tier": tier,
        "proposed_futures_leverage": leverage,
        "score_components": components,
        "score_penalties": {"chop": chop_penalty, "late_entry": round(late_penalty, 4)},
        "atr_15m": atr,
        "lower_high_separation_atr": separation_atr,
        "intervening_support": support,
        "support_broken": below_support,
        "breakdown_held_two_closes": held_breakdown,
        "entry_reason": "support_breakdown"
        if below_support
        else "confirmed_lower_high",
        "bearish_body_atr": body_atr,
        "close_location": close_location,
        "volume_ratio": volume_ratio,
        "coin_return_1h": coin_return,
        "btc_return_1h": btc_return,
        "underperformance_atr": underperformance_atr,
        "hourly_bearish_structure": hourly_bearish,
        "microregime_directional": micro_direction,
        "daily_extension_pct": daily_extension,
        "late_entry_atr": late_entry_atr,
        "next_support": next_support,
        "downside_room_pct": downside,
        "downside_room_tier": "unknown"
        if downside is None
        else "limited"
        if downside < 2
        else "moderate"
        if downside < 4.5
        else "large",
        "optional_data_available": {
            "btc": btc is not None,
            "hourly": hourly is not None,
            "daily": daily is not None,
        },
    }


class DarkKnight:
    """Persist proposed 1–5x leverage without requesting any bot execution.

    Retain the fresh lower-high and BTC/breadth gates for comparison with
    TopGainerShort. Quality (0–100) and downside room are separate measurements.
    Apply the same entry order-book liquidity limits as TopGainerShort.
    No probability claim, outcome model, or symbol-specific fitting is made.
    """

    ALGO = "dark_knight"
    STOP_BUFFER_PCT = 2.0
    TRAILING_PROFIT_PCT = 4.5
    TRAILING_DEVIATION_PCT = 3.0

    def __init__(self, cls: "ContextEvaluator") -> None:
        self.ti = cls
        self._last_emitted: int | None = None

    async def signal(
        self, current_price: float, bb_high: float, bb_mid: float, bb_low: float
    ) -> None:
        if (
            self.ti.market_type != MarketType.FUTURES
            or not isfinite(current_price)
            or current_price <= 0
        ):
            return
        now = time()
        frame = completed_ohlcv(
            self.ti.df_15m, now_ms=now * 1000, interval_ms=900_000, bars=40
        )
        if frame is None:
            return
        pattern = LowerHighPattern.detect(frame)
        if (
            pattern is None
            or pattern["confirmation_open_time"] != frame.open_time.iloc[-1]
        ):
            return
        confirmation = int(pattern["confirmation_open_time"])
        key = (self.ALGO, self.ti.symbol)
        cooldowns = self.ti.strategy_cooldowns
        last_emitted = (
            cooldowns.get(key) if cooldowns is not None else self._last_emitted
        )
        if confirmation == last_emitted or current_price >= pattern["later_high"]:
            return

        btc = completed_ohlcv(
            self.ti.df_btc_15m, now_ms=now * 1000, interval_ms=900_000, bars=40
        )
        btc_pattern = None
        btc_crossings = None
        if btc is not None:
            btc_pattern = confirmed_higher_low(
                [
                    (float(stamp) / 1000, float(low))
                    for stamp, low in zip(btc["close_time"], btc["low"], strict=True)
                ],
                now_seconds=now,
                lookback_samples=40,
                fractal_wing=2,
                max_age_seconds=1800,
                max_gap_seconds=900,
            )
            btc_crossings = price_crossings_six_hours(
                btc,
                float(btc.close.iloc[-1]),
                now_ms=now * 1000,
                interval_ms=900_000,
            )
        breadth = self.ti.market_breadth_data
        breadth_pattern = None
        breadth_crossings = None
        if breadth is not None and len(breadth.timestamp) == len(
            breadth.market_breadth
        ):
            samples = [
                (timestamp_sort_key(stamp), value)
                for stamp, value in zip(
                    breadth.timestamp, breadth.market_breadth, strict=True
                )
            ]
            if all(stamp is not None for stamp, _ in samples):
                breadth_samples = [
                    (stamp, value) for stamp, value in samples if stamp is not None
                ]
                breadth_pattern = confirmed_higher_low(
                    breadth_samples,
                    now_seconds=now,
                    lookback_samples=40,
                    fractal_wing=2,
                    max_age_seconds=1800,
                    max_gap_seconds=1800,
                )
                breadth_crossings = breadth_crossings_six_hours(
                    breadth_samples, now_seconds=now
                )
        if btc_pattern is None and breadth_pattern is None:
            return
        if (
            btc_crossings is not None
            and btc_crossings >= CHOP_CROSSING_THRESHOLD
            and breadth_crossings is not None
            and breadth_crossings >= CHOP_CROSSING_THRESHOLD
        ):
            return
        crossings = price_crossings_six_hours(
            frame, current_price, now_ms=now * 1000, interval_ms=900_000
        )
        if crossings is None or crossings >= CHOP_CROSSING_THRESHOLD:
            return

        hourly = completed_ohlcv(
            self.ti.df_1h, now_ms=now * 1000, interval_ms=3_600_000, bars=40
        )
        daily = completed_ohlcv(
            self.ti.df_1d, now_ms=now * 1000, interval_ms=86_400_000, bars=21
        )
        peaks = swing_positions([float(value) for value in frame.high], high=True)
        earlier_peak, later_peak = peaks[-2:]
        indicators: dict[str, Any] = score_short_setup(
            frame,
            current_price=current_price,
            earlier_peak=earlier_peak,
            later_peak=later_peak,
            btc=btc,
            hourly=hourly,
            daily=daily,
            micro_direction=self.ti.microregime_directional,
            crossings=crossings,
        )
        stop = (
            Decimal(str(pattern["later_high"]))
            * (1 + Decimal(str(self.STOP_BUFFER_PCT)) / 100)
        ).quantize(Decimal(1).scaleb(-self.ti.price_precision), rounding=ROUND_CEILING)
        stop_pct = float(
            ((stop / Decimal(str(current_price)) - 1) * 100).quantize(
                Decimal("0.0001"), rounding=ROUND_CEILING
            )
        )
        if not 0 < stop_pct < 100:
            return
        leverage = indicators["proposed_futures_leverage"]
        indicators.update(
            {
                "observation_only": True,
                "evaluation_timestamp_ms": int(now * 1000),
                "signal_candle_open_time": confirmation,
                "lower_high_first_peak": pattern["earlier_high"],
                "lower_high_second_peak": pattern["later_high"],
                "lower_high_drop_pct": pattern["drop_pct"],
                "breadth_higher_low": breadth_pattern,
                "btc_crossings_six_hours": btc_crossings,
                "breadth_crossings_six_hours": breadth_crossings,
                "btc_higher_low": btc_pattern,
                "price_crossings_six_hours": crossings,
                "stop_loss_price_at_signal": float(stop),
                "stop_loss_pct": stop_pct,
                "futures_leverage": leverage,
                "leverage_tiers": [
                    {"minimum_quality": threshold, "leverage": level, "tier": tier}
                    for threshold, level, tier in LEVERAGE_TIERS
                ],
                "macroregime_directional": self.ti.macroregime_directional,
            }
        )
        value = SignalsConsumer(
            direction=Position.short.value.upper(),
            autotrade=False,
            current_price=current_price,
            score=indicators["quality_score"],
            bot_params=BotBase(
                pair=self.ti.symbol,
                name=self.ALGO,
                position=Position.short,
                market_type=MarketType.FUTURES,
                futures_leverage=leverage,
                fiat_order_size=self.ti.at_consumer.autotrade_settings.base_order_size,
                cooldown=60,
                stop_loss=stop_pct,
                dynamic_position_sizing=True,
                dynamic_trailing=False,
                take_profit=self.TRAILING_PROFIT_PCT,
                trailing=True,
                trailing_profit=self.TRAILING_PROFIT_PCT,
                trailing_deviation=self.TRAILING_DEVIATION_PCT,
                recovery_params=None,
            ),
            bb_spreads=HABollinguerSpread(
                bb_high=bb_high, bb_mid=bb_mid, bb_low=bb_low
            ),
        )
        assert value.bot_params is not None
        liquidity = await self.ti.assess_futures_liquidity(
            value.bot_params.fiat_order_size * value.bot_params.futures_leverage
        )
        if not liquidity.allowed:
            logging.info(
                "%s %s skipped: order_book_liquidity=%s",
                self.ALGO,
                self.ti.symbol,
                asdict(liquidity),
            )
            return
        indicators["order_book_liquidity"] = asdict(liquidity)
        room = indicators["downside_room_pct"]
        room_text = (
            "unknown"
            if room is None
            else f"{room:.2f}% ({indicators['downside_room_tier']})"
        )
        components = ", ".join(
            f"{name}: {points:g}"
            for name, points in indicators["score_components"].items()
        )
        message = f"""
            - [{self.ti.config.env}] <strong>#{self.ALGO} algorithm</strong> #{self.ti.symbol}
            - Action: SHORT OBSERVATION
            - Strategy: Dark Knight
            - Current price: {current_price}
            - Quality score: {indicators["quality_score"]}/100 ({indicators["quality_tier"]}); heuristic, not probability
            - Components: {components}
            - Penalties: chop {indicators["score_penalties"]["chop"]:g}, late entry {indicators["score_penalties"]["late_entry"]:g}
            - Six-hour crossings: coin {crossings}, BTC {btc_crossings if btc_crossings is not None else "unknown"}, breadth {breadth_crossings if breadth_crossings is not None else "unknown"}
            - Proposed futures leverage: {leverage}x
            - Entry evidence: {indicators["entry_reason"]}
            - Downside room to support: {room_text}; not a return forecast
            - Initial reduction threshold: {stop} ({stop_pct}%)
            - Scoring version: {indicators["scoring_version"]}
            - Autotrade is disabled: calibration observations only
            {self.ti.regime_telegram_lines()}
        """
        await self.ti.dispatch_signal_record(value=value, indicators=indicators)
        if value.bot_params is None or value.bot_params.signal_id is None:
            return
        # Mark after persistence, before notification: never create a second
        # observation merely because Telegram failed. Independent strategy key.
        self._last_emitted = confirmation
        if cooldowns is not None:
            cooldowns[key] = confirmation
        await self.ti.telegram_consumer.dispatch_signal(message)

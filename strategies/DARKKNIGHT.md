# Dark Knight leverage experiment

`strategies/dark_knight.py` runs independently beside `top_gainer_short` on the
15-minute pipeline. It persists `dark_knight` signals and sends identifiable
Telegram observations. **It never calls autotrading**, in any environment;
`bot_params.futures_leverage` is a proposal for calibration, not an executed trade.
The existing Top Gainer Short score and execution logic are not reused or changed.

The `dark_knight_v2` score is a heuristic quality measure from 0 to 100,
not a probability or a score directly comparable to other strategies. It retains
the fresh confirmed lower-high trigger, BTC-or-breadth higher-low confirmation,
and rejection at three six-hour coin-price crossings. BTC and market breadth
also have six-hour crossing counts; their chop gate rejects only when both
counts reach three. It does not wait for a later
retest or revisit a pattern on subsequent candles. More confirmation can earn
points when already present in the two candles after the second peak.

| Evidence | Maximum points | v1 rule |
| --- | ---: | --- |
| Bearish structure | 30 | Peak separation reaches 10 points at 2 ATR; 10 for a close more than 0.25 ATR below the intervening trough; 10 for two such closes. Current price must remain below support for breakdown points. |
| Selling pressure | 20 | Bearish body / ATR, capped at 1, times close location gives up to 10; bearish volume relative to the prior 20-bar median gives another 10 at 2x volume. |
| Relative weakness | 20 | Aligned one-hour coin return is negative while BTC is positive; 20 points at 2 ATR-equivalents of underperformance. |
| Timeframe agreement | 15 | 10 for hourly lower highs and lower lows; 5 for `ContextEvaluator.microregime_directional == DOWN`. |
| Prior extension | 5 | Up to 5 for a second peak 100% above the median of 14 daily closes preceding the last 7 completed days. Top-gainer rank is not used. |

ATR is the simple mean of the last 14 completed true ranges. Each six-hour
crossing subtracts 5 points. Entry more than 2 ATR below the intervening support
subtracts 5 points per additional ATR, capped at 15. Missing or invalid optional
BTC/hourly/daily evidence earns no points; weights are never redistributed.
The coin's 40 completed OHLCV bars must be fresh, continuous and valid. Hourly
and daily evidence requires 40 and 21 valid completed bars respectively. A BTC
higher low alone contributes no relative-weakness points.

| Quality score | Proposed futures leverage |
| --- | ---: |
| 0 to below 35 | 1x |
| 35 to below 50 | 2x |
| 50 to below 65 | 3x |
| 65 to below 80 | 4x |
| 80 to 100 | 5x |

`strategies/dark_knight.py` holds the strategy, scoring rules and `LEVERAGE_TIERS`. Change the
scoring version when tuning these rules so observations remain distinguishable.
Each signal saves the component scores, penalties, quality tier, proposed
leverage, market evidence, timestamps and scoring version. The closest confirmed
15m/hourly swing low below entry supplies separate downside room: under 2% is
limited, 2% to below 4.5% moderate, and 4.5%+ large. Missing support is unknown.
This is a candidate price distance, not an expected return, and does not change
the leverage proposal. The 2% buffer above the lower high is recorded as an
initial dynamic reduction threshold, not a guaranteed full-position stop.

Only option 2 is implemented: deterministic rules and proposed leverage tiers.
There is no outcome tracking, probability fitting, per-coin model or automated
calibration. The tier thresholds remain provisional and can be manually tuned.
Observation records alone do not measure realized strategy PnL.

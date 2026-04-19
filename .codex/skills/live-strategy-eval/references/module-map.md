# Module Map

Use this file when the live probe results need to be tied back to the code.

## Shared Market Context

- `market_regime/market_state_store.py`
  - Rolling closed-candle storage for each symbol.
- `market_regime/live_market_context_accumulator.py`
  - Builds the live cross-sectional market snapshot.
  - Requires BTC plus at least 40 fresh symbols for a timestamp.
- `market_regime/regime_routing.py`
  - Canonical helpers for `allows_long_autotrade`, `allows_short_autotrade`, and `resolve_symbol_features`.
- `market_regime/models.py`
  - Shape of `LiveMarketContext` and `SymbolMarketFeatures`.

## Candle Sources

- `pybinbot/apis/kucoin/futures.py`
  - `KucoinFutures.get_ui_klines(...)` is the main live futures candle source.
- `consumers/klines_provider.py`
  - Good reference for how the app builds and refreshes context in production.

## Common Strategy Touchpoints

- `strategies/coinrule/buy_the_dip.py`
  - 15m dip rule with a micro-regime block for `TREND_DOWN`.
- `strategies/liquidation_sweep_pump.py`
  - Needs both a score trigger and bullish routing.
- `strategies/top_gainers_reversal_drop.py`
  - Short setup with regime gating plus 5m local reversal logic.

## Practical Reading Order

1. Strategy file.
2. Matching test file.
3. `market_regime/regime_routing.py`.
4. Probe output from `scripts/live_market_probe.py`.

## Reporting Style

- Quote exact regime names such as `RANGE`, `TREND_UP`, `TREND_DOWN`, or `TRANSITIONAL`.
- Prefer route names from code such as `market_regime_range` or `short_autotrade_allowed`.
- Include the probe timestamp in UTC when calling something "current".

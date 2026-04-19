---
name: live-strategy-eval
description: Use this skill when the user wants an actual live-data test of a Binquant strategy, wants to know the current market or micro regime, wants to see which filter is suppressing signals, or wants fresh KuCoin futures evidence instead of reasoning from code alone.
---

# Live Strategy Eval

## Overview

This skill turns the ad hoc "check it on live data" workflow into a repeatable process for Binquant strategies. It requires a strategy Python file as input, validates that the file exposes exactly one top-level class with a `signal` method, then rebuilds the repo's 15m market-regime snapshot from fresh KuCoin futures candles and maps the live context back to the strategy's routing and entry filters.

## When To Use It

- The user asks for a live-data test, current regime, or recent live evidence.
- The user asks whether a strategy is being suppressed by market regime or micro regime.
- The user wants to know if recent alerts look sensible.
- The user asks whether a strategy fits range, trend, or transitional conditions right now.

## Workflow

1. Read the target strategy and its tests first. Focus on the actual emit path, routing helper, cooldown rules, and any explicit regime gates.
2. Validate the strategy file shape with the helper script. The expected contract is:
   - one top-level class in the file
   - that class defines a `signal` method
   If the file does not match this shape, stop and tell the user exactly what was wrong.
3. Activate the Binbot API virtualenv before running Python:

```sh
source /Users/carkod/binbot/api/.venv/bin/activate
```

4. Rebuild fresh market context with the helper script:

```sh
python /Users/carkod/binquant/.agents/skills/live-strategy-eval/scripts/live_market_probe.py --strategy /Users/carkod/binquant/strategies/coinrule/buy_the_dip.py --symbol-limit 55 --focus-symbols XBTUSDTM
```

5. Use the probe output as the market-context layer:
   - `latest_context.market_regime`
   - `latest_context.market_stress_score`
   - `focus_symbols[*].micro_regime`
   - `focus_symbols[*].long_autotrade_allowed`
   - `focus_symbols[*].short_autotrade_allowed`
6. For symbol-specific questions, add the relevant symbol to `--focus-symbols`.
7. For suppression questions, state exactly which filter is blocking the strategy, using the code's own route names when possible.
8. For quality questions, report exact timestamps, sample size, and caveats. Do not describe results as "live" unless you actually ran the probe.

## Strategy Notes

- The helper script rebuilds the shared 15m regime layer. If the strategy uses 5m entry logic, pair the probe output with the strategy's 5m indicator path.
- `buy_the_dip` is a dip-entry strategy, so treat it as a "buy weakness into a non-bearish context" setup rather than a deep-trend-down continuation setup.
- `top_gainers_reversal_drop` is short-biased and should usually be discussed in terms of both local reversal conditions and short-side regime routing.
- `liquidation_sweep_pump` is unusually sensitive to context because it needs both the score trigger and bullish routing.

## Commands

Snapshot a broad futures universe:

```sh
source /Users/carkod/binbot/api/.venv/bin/activate
python /Users/carkod/binquant/.agents/skills/live-strategy-eval/scripts/live_market_probe.py --strategy /Users/carkod/binquant/strategies/coinrule/buy_the_dip.py --symbol-limit 55 --focus-symbols XBTUSDTM,SSVUSDTM
```

Use a manual universe when you only care about a few symbols:

```sh
source /Users/carkod/binbot/api/.venv/bin/activate
python /Users/carkod/binquant/.agents/skills/live-strategy-eval/scripts/live_market_probe.py --strategy /Users/carkod/binquant/strategies/coinrule/buy_the_dip.py --symbols XBTUSDTM,SSVUSDTM,NUMIUSDTM --focus-symbols SSVUSDTM,NUMIUSDTM
```

Read [references/module-map.md](references/module-map.md) when you need the exact repo touchpoints behind the live probe.

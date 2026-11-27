# Binquant (beta)

The [Binbot](https://github.com/carkod/binbot) quantitative analyses tool.

Performs technical, statistical, AI analysis and feeds it to the signal and telegram systems to eventually create autotrades for Binbot.

`notifiers/` handles Telegram delivery and price-pattern notifications.
Notifications respect `telegram_signals` and suppress duplicates.
Telegram transport is provided by the published pybinbot package.

Potential replacement of binbot-research, now running as a single-process websocket ingest and strategy pipeline connected by an in-memory `asyncio.Queue`.

<img width="2067" height="1885" alt="Binbot architecture-2025-10-26-144727" src="https://github.com/user-attachments/assets/cd3b76a4-0653-421c-b8e0-24685b7d6dd8" />

`top_gainer_short` and `dark_knight` check KuCoin USDT futures order-book
liquidity before recording or notifying a signal. They require a spread of at
most 0.3% and at least 5,000 USDT on **each** side within 0.5% of the midpoint,
or 20 times the proposed position notional if larger. Notional is margin times
leverage; book quantities are contracts and use the symbol's contract multiplier.
Only the top 100 levels count. Missing, malformed, or more than 10-second-old
snapshots block the signal; a request has a 3-second waiting limit. A snapshot
can be reused for 2 seconds across these strategies within one evaluation.
Limits live in `shared/order_book_liquidity.py`; accepted measurements are saved
in signal indicators and rejected checks are logged. A snapshot cannot guarantee
liquidity will remain available when a bot executes or later exits.

`top_gainer_short` requires falling CVD for both the contract and the aggregate
KuCoin futures market: each linear regression must have a negative slope and
Pearson r <= -0.70. It reads `/charts/cvd-symbol` and `/charts/cvd-aggregate`
concurrently after the existing entry and liquidity checks pass. If either
assessment fails this condition or is unavailable, the strategy returns without
persisting a signal, sending Telegram, or triggering autotrade. Rejected CVD
checks do not consume the setup, so it can be retried later. Accepted assessments
are saved as `indicators.cvd_symbol` and `indicators.cvd_aggregate` and displayed
in Telegram. CVD does not change confidence score, leverage or margin.

Each assessment fits a line to the latest continuous segment of 5–12 completed
observations in a 15-bucket request. Contract buckets are one minute; aggregate
buckets are 15 minutes. Slopes use USDT/minute for both. A positive slope with
Pearson r >= 0.70 is `rising`, a negative slope with r <= -0.70 is `falling`,
and other valid results are `no_direction`. The threshold measures consistency,
not movement size or trading confidence. Constant series have slope and r zero.
Stale checkpoints, insufficient coverage and request failures produce
`unavailable`, preserving the reason rather than inventing a flat reading.
Trailing unfinished buckets are excluded; fits never cross gaps, multiplier or
universe changes, or a CVD reset. Thresholds and calculation rules live in
`shared/cvd.py`.

The CVD clients and response models require published pybinbot >= 2.1.4;
Binquant's minimum dependency and lockfile include that release. `make format`
and `make test` use the published package from the project environment.

## Development

1. Use the [docker-compose.yml](https://github.com/carkod/binbot/blob/master/docker-compose.yml) from [Binbot project](https://github.com/carkod/binbot).
2. Use the settings provided in the .vscode folder and run the debugger for Binbot: Api
3. Run Binquant as a single process through `main.py`, which starts websocket ingestion and consumes klines from the shared in-memory queue in the same event loop.

### Parallel calculations

The worker keeps two persistent calculation threads shared by candle indicators
and `top_gainer_short`. Each incoming message's 5-minute and 15-minute candle
frames are built and enriched independently. The strategy then checks its fresh
lower high in a worker and, if present, calculates breadth and BTC higher lows
and six-hour price crossings concurrently. Daily stretch context also runs in a
worker after the entry checks pass. These calculations use one evaluation time
and private input snapshots, including separate DataFrames for each job.

At most two snapshots are submitted at once; additional submissions wait before
copying inputs. Cancellation retains capacity until the running calculation
finishes. The pool is shut down when the application exits.

Messages are still processed one at a time. Strategy state, cooldowns, portfolio
selection and trading decisions remain on the main asyncio loop, preserving
their ordering. The websocket input queue is separate and retains its existing
behaviour.

Binquant requires free-threaded Python 3.15. The `.python-version` file selects
`3.15t` for local development, CI and the Docker dependency build. Use a recent uv
release to install the runtime and dependencies:

```sh
uv sync --locked --all-extras --dev
uv run --locked python main.py
```

Startup checks that the GIL is disabled before connecting to the services, then
logs the worker count and GIL state. A native dependency can re-enable it later;
do not force incompatible extensions to run with `PYTHON_GIL=0`. CI runs the tests
with free-threaded Python and checks that the suite leaves the GIL disabled.

The default Docker build also uses free-threaded Python:

```sh
docker build -t binquant:local .
```

Measure queue lag and peak memory before deploying this variant. The calculation
benchmark does not represent the throughput of the entire trading pipeline.

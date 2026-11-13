# Binquant (beta)

The [Binbot](https://github.com/carkod/binbot) quantitative analyses tool.

Performs technical, statistical, AI analysis and feeds it to the signal and telegram systems to eventually create autotrades for Binbot.

Telegram signal policy lives in `shared/telegram_signal_notifier.py`.
Delivery and credential loading are provided by `TelegramDelivery` in pybinbot;
the Telegram library is installed transitively. Deploy pybinbot 1.13.7 with this
change and refresh this application's lockfile after publishing that release.

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

## Development

1. Use the [docker-compose.yml](https://github.com/carkod/binbot/blob/master/docker-compose.yml) from [Binbot project](https://github.com/carkod/binbot).
2. Use the settings provided in the .vscode folder and run the debugger for Binbot: Api
3. Run Binquant as a single process through `main.py`, which starts websocket ingestion and consumes klines from the shared in-memory queue in the same event loop.

### Parallel candle calculations

The worker keeps two persistent calculation threads. Each incoming message's
5-minute and 15-minute candle frames are built and enriched independently. At
most two snapshots are submitted at once; additional submissions wait before
copying candles. Cancellation retains capacity until the running calculation
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

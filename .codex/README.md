# Codex Workspace Notes

This is the `binquant` repository: the quantitative analysis, signal,
producer/consumer, and Kafka-oriented project.

Sibling repository: `/Users/carkod/binbot`

How to tell this repo apart from `binbot`:

- If the request mentions `algorithms/`, `producers/`, `consumers/`,
  `signal_collector`, `coinrule`, or tests like
  `tests/test_coinrule_price_tracker.py`, it is probably `binquant`.
- If the request mentions `api/`, `terminal/`, routes, autotrade settings
  endpoints, or frontend/API work, it is probably `binbot`.

Helpful user prompt pattern:

```sh
In binquant on branch signal-consumer-msg, can you fix tests/test_coinrule_price_tracker.py?
```

Before running Python commands, activate the project virtualenv:

```sh
source /Users/carkod/binquant/.venv/bin/activate
```

This repository currently uses a root-level virtualenv at `.venv`.

If a Python command fails with `ModuleNotFoundError` or a similar missing-package
bootstrap error like `No module named 'pandera'`, do not stop at the import
failure. Follow the repository setup path first, then retry:

```sh
cd /Users/carkod/binquant
source .venv/bin/activate
uv sync --extra dev
```

After that, run the command again using the project environment. The repo
tooling already expects this workflow:

- `Makefile` uses `uv run pytest ...` for tests
- `Makefile` uses `uv sync --extra dev` to install development dependencies
- Prefer the `Makefile` entrypoints when there are follow-up issues with
  verification or formatting

In short: if dependencies are missing, bootstrap the environment according to
the repo instructions before reporting that tests cannot run.

Preferred repo commands:

```sh
make test
make format
```

Use `make test` to run the supported test command for the project, and use
`make format` to run the repo's formatting, lint, and type-check flow. When a
direct `pytest` or tool invocation fails in a confusing way, prefer retrying
through the `Makefile` before giving up.

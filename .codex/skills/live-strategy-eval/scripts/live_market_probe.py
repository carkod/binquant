#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_BINQUANT_ROOT = Path("/Users/carkod/binquant")
DEFAULT_PYBINBOT_ROOT = Path("/Users/carkod/pybinbot")
DEFAULT_BINBOT_VENV = Path("/Users/carkod/binbot/api/.venv")


def parse_symbols(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def ensure_import_paths(binquant_root: Path, pybinbot_root: Path) -> None:
    for path in (binquant_root, pybinbot_root):
        resolved = str(path.resolve())
        if resolved not in sys.path:
            sys.path.insert(0, resolved)


def isoformat_ms(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat()


def inspect_strategy_file(
    strategy_path: Path,
    binquant_root: Path,
) -> dict[str, Any]:
    resolved_path = strategy_path.resolve()
    if not resolved_path.exists():
        raise RuntimeError(f"Strategy file does not exist: {resolved_path}")
    if resolved_path.suffix != ".py":
        raise RuntimeError(
            f"Strategy path must point to a Python file, got: {resolved_path}"
        )

    source = resolved_path.read_text(encoding="utf-8")
    parsed = ast.parse(source, filename=str(resolved_path))
    top_level_classes = [
        node for node in parsed.body if isinstance(node, ast.ClassDef)
    ]

    if not top_level_classes:
        raise RuntimeError(
            "Strategy file must define exactly one top-level class with a `signal` "
            f"method, but none were found in {resolved_path}"
        )
    if len(top_level_classes) != 1:
        class_names = ", ".join(node.name for node in top_level_classes)
        raise RuntimeError(
            "Strategy file must define exactly one top-level class with a `signal` "
            f"method, but found {len(top_level_classes)} classes in {resolved_path}: "
            f"{class_names}"
        )

    class_node = top_level_classes[0]
    methods = [
        node.name
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    if "signal" not in methods:
        raise RuntimeError(
            "Strategy class must define a `signal` method. "
            f"Found class `{class_node.name}` in {resolved_path} with methods: "
            f"{', '.join(methods) if methods else 'none'}"
        )

    try:
        relative_path = resolved_path.relative_to(binquant_root.resolve())
        relative_str = str(relative_path)
    except ValueError:
        relative_str = str(resolved_path)

    return {
        "path": str(resolved_path),
        "relative_path": relative_str,
        "class_name": class_node.name,
        "methods": sorted(methods),
        "has_signal_method": True,
    }


def normalize_candles(raw_klines: list[list[Any]]) -> list[dict[str, float | int]]:
    rows: list[dict[str, float | int]] = []
    for kline in raw_klines:
        if len(kline) < 6:
            continue
        close_time = int(kline[6] if len(kline) > 6 else kline[0])
        rows.append(
            {
                "timestamp": close_time,
                "open": float(kline[1]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[4]),
                "volume": float(kline[5]),
            }
        )
    rows.sort(key=lambda item: int(item["timestamp"]))
    return rows


def summarize_context(context: Any) -> dict[str, Any]:
    return {
        "timestamp": context.timestamp,
        "timestamp_iso": isoformat_ms(context.timestamp),
        "market_regime": context.market_regime,
        "previous_market_regime": context.previous_market_regime,
        "market_regime_transition": context.market_regime_transition,
        "market_regime_transition_strength": round(
            float(context.market_regime_transition_strength), 4
        ),
        "regime_is_transitioning": bool(context.regime_is_transitioning),
        "fresh_count": int(context.fresh_count),
        "total_tracked_symbols": int(context.total_tracked_symbols),
        "coverage_ratio": round(float(context.coverage_ratio), 4),
        "btc_symbol": context.btc_symbol,
        "btc_present": bool(context.btc_present),
        "btc_return": round(float(context.btc_return), 6),
        "btc_regime_score": round(float(context.btc_regime_score), 4),
        "market_stress_score": round(float(context.market_stress_score), 4),
        "long_tailwind": round(float(context.long_tailwind), 4),
        "short_tailwind": round(float(context.short_tailwind), 4),
        "long_regime_score": round(float(context.long_regime_score), 4),
        "short_regime_score": round(float(context.short_regime_score), 4),
        "range_regime_score": round(float(context.range_regime_score), 4),
        "stress_regime_score": round(float(context.stress_regime_score), 4),
        "advancers_ratio": round(float(context.advancers_ratio), 4),
        "decliners_ratio": round(float(context.decliners_ratio), 4),
        "average_return": round(float(context.average_return), 6),
        "average_relative_strength_vs_btc": round(
            float(context.average_relative_strength_vs_btc), 6
        ),
    }


def summarize_symbol(
    context: Any,
    symbol: str,
    allows_long_autotrade: Any,
    allows_short_autotrade: Any,
) -> dict[str, Any]:
    features = context.get_symbol_features(symbol)
    if features is None:
        return {"symbol": symbol, "available": False}

    return {
        "symbol": symbol,
        "available": True,
        "timestamp": int(features.timestamp),
        "timestamp_iso": isoformat_ms(features.timestamp),
        "close": round(float(features.close), 8),
        "return_pct": round(float(features.return_pct), 6),
        "trend_score": round(float(features.trend_score), 6),
        "relative_strength_vs_btc": round(float(features.relative_strength_vs_btc), 6),
        "atr_pct": round(float(features.atr_pct), 6),
        "bb_width": round(float(features.bb_width), 6),
        "above_ema20": bool(features.above_ema20),
        "above_ema50": bool(features.above_ema50),
        "micro_regime": features.micro_regime,
        "micro_regime_strength": round(float(features.micro_regime_strength), 4),
        "micro_regime_transition": features.micro_regime_transition,
        "micro_regime_transition_strength": round(
            float(features.micro_regime_transition_strength), 4
        ),
        "long_autotrade_allowed": bool(allows_long_autotrade(context, symbol)),
        "short_autotrade_allowed": bool(allows_short_autotrade(context, symbol)),
    }


def build_recent_counts(
    contexts: list[Any], recent_contexts: int
) -> dict[str, dict[str, int]]:
    recent = contexts[-recent_contexts:] if recent_contexts > 0 else contexts
    regime_counts = Counter(
        context.market_regime or "UNAVAILABLE" for context in recent
    )
    transition_counts = Counter(
        context.market_regime_transition or "NONE" for context in recent
    )
    return {
        "market_regime_counts": dict(sorted(regime_counts.items())),
        "market_transition_counts": dict(sorted(transition_counts.items())),
    }


def build_universe(
    explicit_symbols: list[str],
    focus_symbols: list[str],
    symbol_limit: int,
    btc_symbol: str,
    config: Any,
    binbot_api_cls: Any,
    kucoin_api: Any,
) -> tuple[list[str], str]:
    seed_symbols = [btc_symbol, *focus_symbols, *explicit_symbols]
    deduped_seed = list(dict.fromkeys(seed_symbols))
    if explicit_symbols:
        return deduped_seed, "manual"

    try:
        binbot_api = binbot_api_cls(
            base_url=config.backend_domain,
            service_email=config.service_email,
            service_password=config.service_password,
        )
        records = binbot_api.get_symbols()
        futures_symbols = [
            str(record["id"]).upper()
            for record in records
            if str(record.get("id", "")).upper().endswith("USDTM")
        ]
        futures_symbols = list(dict.fromkeys(sorted(futures_symbols)))
        universe = [symbol for symbol in deduped_seed if symbol in futures_symbols]
        for symbol in futures_symbols:
            if symbol in universe:
                continue
            universe.append(symbol)
            if symbol_limit > 0 and len(universe) >= symbol_limit:
                break
        if btc_symbol not in universe:
            universe.insert(0, btc_symbol)
        return list(dict.fromkeys(universe)), "binbot_api"
    except Exception:
        pass

    try:
        public_symbols = kucoin_api.futures_market_api.get_all_symbols().data
        futures_symbols = [
            str(item.symbol).upper()
            for item in public_symbols
            if str(item.symbol).upper().endswith("USDTM")
            and str(item.quote_currency).upper() == "USDT"
        ]
        futures_symbols = list(dict.fromkeys(sorted(futures_symbols)))
        universe = [symbol for symbol in deduped_seed if symbol in futures_symbols]
        for symbol in futures_symbols:
            if symbol in universe:
                continue
            universe.append(symbol)
            if symbol_limit > 0 and len(universe) >= symbol_limit:
                break
        if btc_symbol not in universe:
            universe.insert(0, btc_symbol)
        return list(dict.fromkeys(universe)), "kucoin_public"
    except Exception:
        if deduped_seed:
            return deduped_seed, "manual_fallback"
        raise


def fetch_histories(
    api: Any,
    symbols: list[str],
    interval: str,
    limit: int,
) -> tuple[dict[str, list[dict[str, float | int]]], dict[str, str]]:
    histories: dict[str, list[dict[str, float | int]]] = {}
    failures: dict[str, str] = {}
    for symbol in symbols:
        try:
            raw_klines = api.get_ui_klines(
                symbol=symbol, interval=interval, limit=limit
            )
            rows = normalize_candles(raw_klines)
            if len(rows) < 2:
                failures[symbol] = "not_enough_candles"
                continue
            histories[symbol] = rows
        except Exception as exc:
            failures[symbol] = str(exc)
    return histories, failures


def rebuild_contexts(
    histories: dict[str, list[dict[str, float | int]]],
    btc_symbol: str,
    max_bars_per_symbol: int,
    market_state_store_cls: Any,
    accumulator_cls: Any,
) -> list[Any]:
    if btc_symbol not in histories:
        raise RuntimeError(f"BTC benchmark symbol {btc_symbol} was not fetched.")

    by_symbol_and_timestamp = {
        symbol: {int(row["timestamp"]): row for row in rows}
        for symbol, rows in histories.items()
    }
    all_timestamps = sorted(
        {int(row["timestamp"]) for rows in histories.values() for row in rows}
    )
    ordered_symbols = [btc_symbol] + [
        symbol for symbol in histories if symbol != btc_symbol
    ]

    store = market_state_store_cls(max_bars_per_symbol=max_bars_per_symbol)
    accumulator = accumulator_cls(state_store=store, btc_symbol=btc_symbol)
    contexts: list[Any] = []

    for timestamp in all_timestamps:
        for symbol in ordered_symbols:
            row = by_symbol_and_timestamp[symbol].get(timestamp)
            if row is None:
                continue
            accumulator.on_closed_candle(symbol=symbol, candle=row)
        context = accumulator.get_context(timestamp)
        if context is not None:
            contexts.append(context)

    return contexts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild Binquant live market context from fresh KuCoin futures candles."
    )
    parser.add_argument(
        "--strategy",
        "--strategy-path",
        dest="strategy_path",
        required=True,
        help="Path to the strategy Python file. The file must expose exactly one top-level class with a `signal` method.",
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated manual futures universe, for example XBTUSDTM,SSVUSDTM,NUMIUSDTM",
    )
    parser.add_argument(
        "--focus-symbols",
        help="Comma-separated symbols to surface in the output summary.",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=55,
        help="Maximum universe size when pulling symbols from Binbot API.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=120,
        help="Number of 15m candles to fetch per symbol.",
    )
    parser.add_argument(
        "--interval",
        default="15min",
        help="KuCoin futures kline interval. The regime layer is tuned for 15m.",
    )
    parser.add_argument(
        "--recent-contexts",
        type=int,
        default=12,
        help="How many recent completed contexts to summarize.",
    )
    parser.add_argument(
        "--btc-symbol",
        default="XBTUSDTM",
        help="Benchmark BTC futures symbol used by the regime layer.",
    )
    parser.add_argument(
        "--binquant-root",
        default=str(DEFAULT_BINQUANT_ROOT),
        help="Path to the Binquant repo root.",
    )
    parser.add_argument(
        "--pybinbot-root",
        default=str(DEFAULT_PYBINBOT_ROOT),
        help="Path to the Pybinbot repo root.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binquant_root = Path(args.binquant_root)
    strategy_info = inspect_strategy_file(
        strategy_path=Path(args.strategy_path),
        binquant_root=binquant_root,
    )
    explicit_symbols = parse_symbols(args.symbols)
    focus_symbols = parse_symbols(args.focus_symbols)

    ensure_import_paths(
        binquant_root=binquant_root,
        pybinbot_root=Path(args.pybinbot_root),
    )

    from pybinbot import BinbotApi, KucoinFutures  # type: ignore
    from market_regime.live_market_context_accumulator import (  # type: ignore
        LiveMarketContextAccumulator,
    )
    from market_regime.market_state_store import MarketStateStore  # type: ignore
    from market_regime.regime_routing import (  # type: ignore
        allows_long_autotrade,
        allows_short_autotrade,
    )
    from shared.config import Config  # type: ignore

    config = Config()
    kucoin_api = KucoinFutures(
        key=config.kucoin_key,
        secret=config.kucoin_secret,
        passphrase=config.kucoin_passphrase,
    )
    universe, universe_source = build_universe(
        explicit_symbols=explicit_symbols,
        focus_symbols=focus_symbols,
        symbol_limit=max(args.symbol_limit, 0),
        btc_symbol=args.btc_symbol.upper(),
        config=config,
        binbot_api_cls=BinbotApi,
        kucoin_api=kucoin_api,
    )
    histories, failures = fetch_histories(
        api=kucoin_api,
        symbols=universe,
        interval=args.interval,
        limit=args.limit,
    )
    contexts = rebuild_contexts(
        histories=histories,
        btc_symbol=args.btc_symbol.upper(),
        max_bars_per_symbol=args.limit,
        market_state_store_cls=MarketStateStore,
        accumulator_cls=LiveMarketContextAccumulator,
    )
    if not contexts:
        raise RuntimeError(
            "No live market contexts were built. Check symbol coverage and candle freshness."
        )

    latest_context = contexts[-1]
    latest_counts = Counter(
        features.micro_regime or "UNAVAILABLE"
        for features in latest_context.symbol_features.values()
    )
    surfaced_symbols = focus_symbols or universe[: min(10, len(universe))]
    surfaced_summary = [
        summarize_symbol(
            context=latest_context,
            symbol=symbol,
            allows_long_autotrade=allows_long_autotrade,
            allows_short_autotrade=allows_short_autotrade,
        )
        for symbol in surfaced_symbols
    ]

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "strategy": strategy_info,
        "python_executable": sys.executable,
        "suggested_venv": str(DEFAULT_BINBOT_VENV),
        "interval": args.interval,
        "candles_per_symbol": args.limit,
        "btc_symbol": args.btc_symbol.upper(),
        "universe_source": universe_source,
        "requested_symbols": universe,
        "fetched_symbols": sorted(histories.keys()),
        "failed_symbols": failures,
        "latest_context": summarize_context(latest_context),
        "latest_micro_regime_counts": dict(sorted(latest_counts.items())),
        "recent_context_summary": build_recent_counts(
            contexts=contexts,
            recent_contexts=args.recent_contexts,
        ),
        "focus_symbols": surfaced_summary,
        "context_window": {
            "first_context_timestamp": contexts[0].timestamp,
            "first_context_timestamp_iso": isoformat_ms(contexts[0].timestamp),
            "latest_context_timestamp": latest_context.timestamp,
            "latest_context_timestamp_iso": isoformat_ms(latest_context.timestamp),
            "context_count": len(contexts),
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Quick baseline analytics for Telegram JSONL exports."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Telegram JSONL export.")
    parser.add_argument("--input", required=True, help="Path to JSONL export file.")
    parser.add_argument("--top-senders", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.input)

    total = 0
    senders: Counter[str] = Counter()
    day_counts: Counter[str] = Counter()
    non_empty = 0

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            total += 1

            sender = row.get("sender_name") or str(row.get("sender_id") or "unknown")
            senders[sender] += 1

            dt = row.get("datetime_utc")
            if dt:
                day = datetime.fromisoformat(dt).date().isoformat()
                day_counts[day] += 1

            if (row.get("text") or "").strip():
                non_empty += 1

    print(f"Total messages: {total}")
    print(f"Non-empty text messages: {non_empty}")

    if day_counts:
        print(f"Date range: {min(day_counts)} to {max(day_counts)}")

    print("Top senders:")
    for sender, count in senders.most_common(args.top_senders):
        print(f"  - {sender}: {count}")

    print("Top days by volume:")
    for day, count in day_counts.most_common(10):
        print(f"  - {day}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Export Telegram messages to JSONL (and optional Markdown) using Telethon."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

from telethon import TelegramClient


@dataclass
class ExportRecord:
    chat: str
    message_id: int
    datetime_utc: str | None
    sender_id: int | None
    sender_name: str | None
    text: str
    reply_to_message_id: int | None
    views: int | None
    forwards: int | None

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Telegram messages to JSONL.")
    parser.add_argument("--chat", help="Chat username/title/id to export.")
    parser.add_argument("--limit", type=int, default=1000, help="Max messages to export.")
    parser.add_argument("--out", help="Output JSONL path.")
    parser.add_argument("--markdown-out", help="Optional Markdown output path.")
    parser.add_argument("--session", default="telegram-export", help="Telethon session name.")
    parser.add_argument(
        "--list-dialogs",
        action="store_true",
        help="List recent dialogs and exit (ignores --chat).",
    )
    return parser


async def _list_dialogs(client: TelegramClient) -> None:
    async for dialog in client.iter_dialogs(limit=100):
        print(f"{dialog.id}\t{dialog.name}")


async def _export_messages(client: TelegramClient, chat: str, limit: int) -> list[ExportRecord]:
    entity = await client.get_entity(chat)
    records: list[ExportRecord] = []

    async for message in client.iter_messages(entity, limit=limit):
        dt = message.date.astimezone(UTC).isoformat() if message.date else None
        sender = await message.get_sender() if message.sender_id else None
        sender_name = getattr(sender, "first_name", None) or getattr(sender, "title", None)

        records.append(
            ExportRecord(
                chat=str(chat),
                message_id=message.id,
                datetime_utc=dt,
                sender_id=message.sender_id,
                sender_name=sender_name,
                text=message.message or "",
                reply_to_message_id=message.reply_to_msg_id,
                views=message.views,
                forwards=message.forwards,
            )
        )

    return records


def _write_jsonl(path: Path, records: list[ExportRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(record.to_json() + "\n")


def _write_markdown(path: Path, records: list[ExportRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("# Telegram Export\n\n")
        for record in records:
            timestamp = record.datetime_utc or "unknown-time"
            sender = record.sender_name or str(record.sender_id) or "unknown-sender"
            text = record.text.replace("\n", " ").strip()
            handle.write(f"- **{timestamp}** · **{sender}** · {text}\n")


async def _run(args: argparse.Namespace) -> int:
    api_id = int(_require_env("TELEGRAM_API_ID"))
    api_hash = _require_env("TELEGRAM_API_HASH")

    client = TelegramClient(args.session, api_id, api_hash)
    await client.start()

    try:
        if args.list_dialogs:
            await _list_dialogs(client)
            return 0

        if not args.chat:
            raise RuntimeError("--chat is required unless --list-dialogs is set.")
        if not args.out:
            raise RuntimeError("--out is required unless --list-dialogs is set.")

        records = await _export_messages(client, args.chat, args.limit)
        _write_jsonl(Path(args.out), records)

        if args.markdown_out:
            _write_markdown(Path(args.markdown_out), records)

        print(f"Exported {len(records)} messages to {args.out}")
        if args.markdown_out:
            print(f"Wrote markdown output to {args.markdown_out}")

    finally:
        await client.disconnect()

    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())

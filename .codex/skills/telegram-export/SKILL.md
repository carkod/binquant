---
name: telegram-export
description: Export Telegram messages from personal chats, groups, or channels into local JSONL/Markdown files using Telegram API credentials. Use when a user asks to retrieve, archive, or prepare Telegram message history for downstream processing, analytics, summarization, compliance review, or another skill.
---

# Telegram Export

Export Telegram message history in a repeatable, analysis-friendly format.

## Quick Start

1. Ensure API credentials are available as environment variables:
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
2. Install dependency: `pip install telethon`
3. Run the exporter script:

```bash
python scripts/export_telegram_messages.py \
  --chat "@channel_or_username" \
  --limit 2000 \
  --out data/telegram/messages.jsonl
```

4. Optional: also generate Markdown for easier reading:

```bash
python scripts/export_telegram_messages.py \
  --chat "@channel_or_username" \
  --limit 1000 \
  --out data/telegram/messages.jsonl \
  --markdown-out data/telegram/messages.md
```

## Workflow

### 1) Resolve target chat

Accept any of:
- username (`@my_group`)
- numeric chat id (`-1001234567890`)
- exact title (if user provides it)

If resolution is ambiguous, list recent dialogs with `--list-dialogs` and ask the user which chat to export.

### 2) Export normalized records

Write one JSON object per line with stable fields:
- `chat`
- `message_id`
- `datetime_utc`
- `sender_id`
- `sender_name`
- `text`
- `reply_to_message_id`
- `views`
- `forwards`

Prefer UTC timestamps and preserve original text exactly.

### 3) Verify output quality

Run a lightweight sanity check:
- confirm file exists and is non-empty
- inspect first/last records
- confirm date ordering is descending (Telegram default) unless user requests ascending

## Output contract

Default output is JSONL, suitable for analysis pipelines and LLM summarization.

When `--markdown-out` is provided, generate a readable document grouped by day.

## Notes

- First run may require interactive login/2FA in terminal.
- Session files are created locally by Telethon; avoid committing them.
- Respect privacy and legal constraints before exporting private chats.

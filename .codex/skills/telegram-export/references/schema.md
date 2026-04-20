# Telegram Export JSONL Schema

Each line in the JSONL output is one message record.

```json
{
  "chat": "@example",
  "message_id": 123,
  "datetime_utc": "2026-04-19T12:34:56+00:00",
  "sender_id": 987654321,
  "sender_name": "Alice",
  "text": "message body",
  "reply_to_message_id": 120,
  "views": 42,
  "forwards": 3
}
```

Guidelines:
- Keep UTC timestamps.
- Keep one message per JSON line.
- Preserve text exactly; do not pre-clean unless user requests.

---
name: telegram-analyzer
description: Analyze exported Telegram message datasets (JSONL/Markdown/text) for themes, sentiment signals, activity patterns, and action items. Use after message export when a user asks for summaries, trend analysis, topic extraction, conversation health checks, or behavioral insights.
---

# Telegram Analyzer

Analyze Telegram exports and produce structured insights with reproducible steps.

## Input expectations

Preferred input: JSONL produced by `telegram-export` with one record per line.

Also acceptable:
- plain text transcripts
- markdown chat dumps

If input is not normalized, first convert it to a table with columns:
`datetime_utc`, `sender`, `text`.

## Analysis workflow

1. **Load and profile**
   - count messages
   - date range
   - top senders
   - empty/system message proportion

2. **Extract themes**
   - identify recurring topics from noun phrases/keywords
   - group into 3-10 high-level themes
   - attach representative message snippets (short quotes)

3. **Sentiment and tone scan**
   - classify message-level polarity (positive/neutral/negative)
   - aggregate by sender and by week
   - flag sharp sentiment shifts

4. **Actionability report**
   - detect questions, decisions, TODO-like language
   - compile open items and likely owners

5. **Return concise deliverables**
   - executive summary
   - key metrics table
   - top themes
   - risks/opportunities
   - suggested follow-up prompts

## Optional scripted analysis

Use `scripts/analyze_telegram_jsonl.py` for a quick baseline report before deeper LLM interpretation.

```bash
python scripts/analyze_telegram_jsonl.py --input data/telegram/messages.jsonl
```

## Quality bar

- Always state the analyzed date range.
- Separate observed facts from interpretation.
- If sample is small (<100 messages), state low-confidence caveat.

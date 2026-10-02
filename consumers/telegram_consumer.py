import asyncio
from datetime import timedelta
import hashlib
import json
import logging
import re
import time

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut
from telegram.helpers import escape


class TelegramConsumer:
    _ALLOWED_HTML_TAGS = ("b", "strong", "i", "em", "u", "s", "code", "pre", "a")
    _ALLOWED_SIGNAL_ALGORITHMS = frozenset(
        {
            "top_gainer_breadth",
            "top_loser_breadth",
            "top_gainer_early_momentum",
            "top_loser_early_momentum",
        }
    )
    _MIN_SEND_INTERVAL_SECONDS = 1.0
    _RETRY_AFTER_PAD_SECONDS = 2.0
    _SIGNAL_DEDUPE_SECONDS = 900.0

    def __init__(self, token, chat_id, is_enabled: bool = True):
        self.token = token
        self.chat_id = chat_id
        self.bot = Bot(token=self.token)
        self._send_lock = asyncio.Lock()
        self.is_enabled = is_enabled
        self._min_send_interval_seconds = self._MIN_SEND_INTERVAL_SECONDS
        self._retry_after_pad_seconds = self._RETRY_AFTER_PAD_SECONDS
        self._signal_dedupe_seconds = self._SIGNAL_DEDUPE_SECONDS
        self._last_send_at = 0.0
        self._recent_signal_keys: dict[str, float] = {}
        self._pending_signal_keys: set[str] = set()

    def parse_signal(self, result):
        payload = json.loads(result)
        message = payload.get("msg", None)
        if not message:
            return
        return message

    def _sanitize_html(self, message: str) -> str:
        """
        Escape raw HTML-sensitive characters while preserving a small set of
        Telegram-supported formatting tags used by the app.
        """
        sanitized = escape(message)

        for tag in self._ALLOWED_HTML_TAGS:
            sanitized = sanitized.replace(f"&lt;{tag}&gt;", f"<{tag}>")
            sanitized = sanitized.replace(f"&lt;/{tag}&gt;", f"</{tag}>")

        # Preserve simple attributes on pre/code tags only if they were
        # intentionally provided, e.g. <pre language="python">.
        sanitized = re.sub(
            r"&lt;(pre|code)\s+([^&]*)&gt;",
            lambda match: f"<{match.group(1)} {match.group(2)}>",
            sanitized,
        )
        sanitized = re.sub(
            r"&lt;a\s+href=(?:&#x27;|&quot;)(.+?)(?:&#x27;|&quot;)&gt;",
            lambda match: f'<a href="{match.group(1)}">',
            sanitized,
        )

        # Preserve comparison operators and other already-escaped text entities
        # provided by upstream message builders, e.g. "&lt;" in algorithm text.
        sanitized = re.sub(
            r"&amp;(lt|gt|amp|quot|#x27);",
            lambda match: f"&{match.group(1)};",
            sanitized,
        )

        return sanitized

    def _clean_signal_message(self, message: str) -> str:
        lines = [line.strip() for line in message.splitlines() if line.strip()]
        return "\n".join(lines)

    def _signal_dedupe_key(self, cleaned_message: str) -> str:
        hashtags = re.findall(r"#([A-Za-z0-9_]+)", cleaned_message)
        symbol = hashtags[-1] if hashtags else ""
        algo_match = re.search(
            r"<strong>#([^<\s]+)\s+algorithm</strong>", cleaned_message
        )
        algo = algo_match.group(1) if algo_match else ""

        fields = {
            "action": self._message_field(cleaned_message, "Action"),
            "strategy": self._message_field(cleaned_message, "Strategy"),
            "route": self._message_field(cleaned_message, "Autotrade route"),
            "autotrade": "enabled"
            if "Autotrade is enabled" in cleaned_message
            else "disabled"
            if "Autotrade is disabled" in cleaned_message
            else "",
        }
        key_parts = [algo, symbol, *fields.values()]
        if any(key_parts):
            return "|".join(key_parts)
        return hashlib.sha1(cleaned_message.encode("utf-8")).hexdigest()

    def _signal_algorithm(self, cleaned_message: str) -> str | None:
        match = re.search(r"<strong>#([^<\s]+)\s+algorithm</strong>", cleaned_message)
        return match.group(1) if match else None

    def _message_field(self, cleaned_message: str, label: str) -> str:
        match = re.search(rf"^- {re.escape(label)}:\s*(.+)$", cleaned_message, re.M)
        return match.group(1).strip() if match else ""

    def _drop_duplicate_signal(self, signal_key: str) -> bool:
        if self._signal_dedupe_seconds <= 0:
            if signal_key in self._pending_signal_keys:
                logging.info("Telegram duplicate signal already pending; skipping")
                return True
            self._pending_signal_keys.add(signal_key)
            return False

        now = time.monotonic()
        expired_keys = [
            key
            for key, sent_at in self._recent_signal_keys.items()
            if now - sent_at >= self._signal_dedupe_seconds
        ]
        for key in expired_keys:
            self._recent_signal_keys.pop(key, None)

        if signal_key in self._pending_signal_keys:
            logging.info("Telegram duplicate signal already pending; skipping")
            return True

        last_sent_at = self._recent_signal_keys.get(signal_key)
        if last_sent_at is not None:
            logging.info("Telegram duplicate signal inside cooldown; skipping")
            return True

        self._pending_signal_keys.add(signal_key)
        return False

    async def _sleep_for_send_interval(self) -> None:
        if self._min_send_interval_seconds <= 0 or self._last_send_at <= 0:
            return
        elapsed = time.monotonic() - self._last_send_at
        sleep_seconds = self._min_send_interval_seconds - elapsed
        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)

    def _retry_after_seconds(self, error: RetryAfter) -> float:
        retry_after = error.retry_after
        if isinstance(retry_after, timedelta):
            return retry_after.total_seconds()
        return float(retry_after)

    async def send_msg(self, message: str) -> None:
        async with self._send_lock:
            while True:
                await self._sleep_for_send_interval()
                try:
                    await self.bot.send_message(
                        self.chat_id,
                        text=self._sanitize_html(message),
                        parse_mode=ParseMode.HTML,
                    )
                    self._last_send_at = time.monotonic()
                    return
                except RetryAfter as e:
                    retry_after = self._retry_after_seconds(e)
                    sleep_seconds = retry_after + self._retry_after_pad_seconds
                    logging.warning(
                        "Telegram flood control active; retrying signal in %.1f seconds",
                        sleep_seconds,
                    )
                    await asyncio.sleep(sleep_seconds)

    async def send_signal(self, message: str) -> bool:
        try:
            cleaned_message = self._clean_signal_message(message)
            if not cleaned_message:
                return False
            await self.send_msg(cleaned_message)
            return True
        except TimedOut as e:
            logging.warning("Telegram signal timed out, skipping: %s", e)
        except Exception as e:
            logging.error(f"Error sending telegram signal: {e}")
            logging.error(f"Original message: {message}")
        return False

    async def dispatch_signal(self, message: str) -> bool:
        """Send an allowed strategy notification before returning to its caller."""
        if not self.is_enabled:
            return False
        cleaned_message = self._clean_signal_message(message)
        if not cleaned_message:
            return False
        algorithm = self._signal_algorithm(cleaned_message)
        if algorithm not in self._ALLOWED_SIGNAL_ALGORITHMS:
            logging.debug(
                "Telegram notifications disabled for strategy %s",
                algorithm or "unknown",
            )
            return False
        signal_key = self._signal_dedupe_key(cleaned_message)
        if self._drop_duplicate_signal(signal_key):
            return False
        try:
            sent = await self.send_signal(cleaned_message)
            if sent:
                self._recent_signal_keys[signal_key] = time.monotonic()
                logging.info(
                    "Telegram signal sent successfully: strategy=%s", algorithm
                )
            return sent
        finally:
            self._pending_signal_keys.discard(signal_key)

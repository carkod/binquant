import asyncio
import json
import logging
import re

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TimedOut
from telegram.helpers import escape


class TelegramConsumer:
    _ALLOWED_HTML_TAGS = ("b", "strong", "i", "em", "u", "s", "code", "pre", "a")

    def __init__(self, token, chat_id, is_enabled: bool = True):
        self.token = token
        self.chat_id = chat_id
        self.bot = Bot(token=self.token)
        self._send_lock = asyncio.Lock()
        self.is_enabled = is_enabled
        # Tasks held here so create_task results aren't garbage-collected
        # before the Telegram round-trip completes.
        self._background_tasks: set[asyncio.Task] = set()

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

    async def send_msg(self, message: str) -> None:
        async with self._send_lock:
            await self.bot.send_message(
                self.chat_id,
                text=self._sanitize_html(message),
                parse_mode=ParseMode.HTML,
            )

    async def send_signal(self, message: str):
        try:
            lines = [
                line.strip() for line in message.splitlines() if line.strip()
            ]  # Strip each line, remove empty ones
            cleaned_message = "\n".join(lines)

            await self.send_msg(cleaned_message)
        except TimedOut as e:
            logging.warning("Telegram signal timed out, skipping: %s", e)
        except Exception as e:
            logging.error(f"Error sending telegram signal: {e}")
            logging.error(f"Original message: {message}")

    def dispatch_signal(self, message: str) -> asyncio.Task | None:
        """
        Fire-and-forget Telegram send. Returns immediately so the caller
        (autotrade path) can run in parallel. Errors are swallowed inside
        send_signal, so the task never propagates exceptions.
        """
        if not self.is_enabled:
            return None
        task = asyncio.create_task(self.send_signal(message))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

import hashlib
import logging
import re
import time

from pybinbot.shared.telegram_delivery import TelegramDelivery


class TelegramSignalNotifier:
    _ALLOWED_SIGNAL_ALGORITHMS = frozenset(
        {
            "top_gainer_short",
            "dark_knight",
            "top_loser_early_momentum",
        }
    )
    _SIGNAL_DEDUPE_SECONDS = 900.0

    def __init__(self, *, is_enabled: bool = True) -> None:
        self.delivery = TelegramDelivery(min_send_interval_seconds=1)
        self.is_enabled = is_enabled
        self._signal_dedupe_seconds = self._SIGNAL_DEDUPE_SECONDS
        self._recent_signal_keys: dict[str, float] = {}
        self._pending_signal_keys: set[str] = set()

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
            sent = await self.delivery.send(cleaned_message)
            if sent:
                self._recent_signal_keys[signal_key] = time.monotonic()
                logging.info(
                    "Telegram signal sent successfully: strategy=%s", algorithm
                )
            return sent
        finally:
            self._pending_signal_keys.discard(signal_key)

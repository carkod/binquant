from __future__ import annotations

import logging
from typing import Any

from pybinbot import BinbotApi


def deactivate_active_bot(
    *,
    binbot_api: BinbotApi,
    algo: str,
    symbol: str,
    source_label: str,
    active_bots: list[dict[str, Any]] | None = None,
) -> str:
    if active_bots is None:
        active_bots = binbot_api.get_bots_by_name(name=algo, symbol=symbol)
    active_bot = active_bots[0] if active_bots else None

    if active_bot is None or not active_bot.get("id"):
        logging.error("No active bot found to deactivate for %s", symbol)
        return "No active bot found to deactivate."

    bot_id = str(active_bot["id"])

    try:
        binbot_api.deactivate_bot(bot_id, algorithmic_close=True)
        deactivate_message: str | list[str] = [
            f"Deactivated active bot from {source_label} signal",
        ]
        binbot_api.submit_bot_event_logs(
            bot_id=bot_id,
            message=deactivate_message,
        )
        return f"Deactivated active bot {bot_id}."
    except Exception as exc:
        logging.exception(
            "%s exit failed to deactivate bot for %s", source_label, symbol
        )
        deactivate_message = f"Bot deactivation failed: {exc}"
        binbot_api.submit_bot_event_logs(
            bot_id=bot_id,
            message=deactivate_message,
        )
        return f"Failed to deactivate active bot {bot_id}: {exc}"

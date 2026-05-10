from __future__ import annotations

import logging
from pybinbot import BinbotApi


class StrategyMixin:
    binbot_api: BinbotApi

    def deactivate_active_bot(
        self,
        bot_id: str,
        symbol: str,
        source_label: str,
    ) -> str:

        try:
            self.binbot_api.deactivate_bot(bot_id, algorithmic_close=True)
            self.binbot_api.submit_bot_event_logs(
                bot_id=bot_id,
                message=[f"Deactivated active bot from {source_label} signal"],
            )
            return f"Deactivated active bot {bot_id}."
        except Exception as exc:
            logging.exception(
                "%s exit failed to deactivate bot for %s", source_label, symbol
            )
            self.binbot_api.submit_bot_event_logs(
                bot_id=bot_id,
                message=[f"Bot deactivation failed: {exc}"],
            )
            return f"Failed to deactivate active bot {bot_id}: {exc}"

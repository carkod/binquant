import json
import os

from telegram import Bot
from telegram.constants import ParseMode


class TelegramConsumer:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_KEY", "")
        self.chat_id = os.getenv("TELEGRAM_USER_ID", "")
        self.bot = Bot(os.getenv("TELEGRAM_BOT_KEY", ""))

    def parse_signal(self, result):
        payload = json.loads(result)
        message = payload.get("msg", "")
        return message

    async def send_msg(self, result):
        async with self.bot:
            message = self.parse_signal(result)
            await self.bot.send_message(
                self.chat_id, text=message, parse_mode=ParseMode.HTML
            )

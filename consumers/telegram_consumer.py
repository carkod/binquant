import json
import logging
import os

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TimedOut

from models.signals import SignalsConsumer


class TelegramConsumer:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_KEY", "")
        self.chat_id = os.getenv("TELEGRAM_USER_ID", "")
        self.bot = Bot(os.getenv("TELEGRAM_BOT_KEY", ""))

    def parse_signal(self, result):
        payload = json.loads(result)
        message = payload.get("msg", None)
        if not message:
            return
        return message

    async def send_msg(self, result):
        async with self.bot:
            message = self.parse_signal(result)
            await self.bot.send_message(
                self.chat_id, text=message, parse_mode=ParseMode.HTML
            )

    async def send_signal(self, result):
        try:
            payload = json.loads(result)
            data = SignalsConsumer(**payload)
            lines = data.msg.splitlines()
            lines = [
                line.strip() for line in lines if line.strip()
            ]  # Strip each line, remove empty ones
            cleaned_message = "\n".join(lines)

            await self.bot.send_message(
                self.chat_id, text=cleaned_message, parse_mode=ParseMode.HTML
            )
        except TimedOut:
            logging.warning("Telegram signal timed out, skipping...")
        except Exception as e:
            logging.error(f"Error sending telegram signal: {e}")

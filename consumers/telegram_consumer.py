from telegram import Bot
import os


class TelegramConsumer:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_KEY", "")
        self.chat_id = os.getenv("TELEGRAM_USER_ID", "")
        self.bot = Bot(os.getenv("TELEGRAM_BOT_KEY", ""))

    async def send_msg(self, msg):
        async with self.bot:
            await self.bot.send_message(self.chat_id, msg)

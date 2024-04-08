
import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
from shared.enums import KafkaTopics
from shared.telegram_bot import TelegramBot


class TelegramConsumer:
    def __init__(self, consumer):
        self.telegram_bot = TelegramBot()
        self.consumer = consumer

    def send_telegram(self, msg):
        """
        Send message with telegram bot
        To avoid Conflict - duplicate Bot error
        /t command will still be available in telegram bot
        """
        if not hasattr(self.telegram_bot, "updater"):
            self.telegram_bot.run_bot()

        self.telegram_bot.send_msg(msg)
        return

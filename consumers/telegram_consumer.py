from aiokafka import AIOKafkaConsumer
from shared.telegram_bot import TelegramBot
import json

class TelegramConsumer:
    def __init__(self, consumer: AIOKafkaConsumer):
        self.telegram_bot = TelegramBot()
        self.consumer = consumer

    def send_telegram(self, msg):
        """
        Send message with telegram bot
        To avoid Conflict - duplicate Bot error
        /t command will still be available in telegram bot
        """
        message = json.loads(msg.value)
        if not hasattr(self.telegram_bot, "updater"):
            self.telegram_bot.run_bot()

        self.telegram_bot.send_msg(message["msg"])


import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
from shared.enums import KafkaTopics
from shared.telegram_bot import TelegramBot


class TelegramConsumer:
    def __init__(self):
        self.telegram_bot = TelegramBot()
        self.consumer = AIOKafkaConsumer(
            KafkaTopics.signals.value,
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m),
        )

    async def get_future_tasks(self):
        """
        Handles consumption as Futures (coroutines)
        then triggers in main with all the other consumer tasks
        """
        tasks = []
        async for result in self.consumer:
            msg = json.loads(result.value)
            tasks.append(asyncio.create_task(self.send_telegram(msg)))
        
        return tasks

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

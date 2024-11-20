import json
import os
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from consumers.autotrade_consumer import AutotradeConsumer
from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider
from aiokafka.errors import UnknownMemberIdError, RequestTimedOutError


async def data_process_pipe():
    try:
        consumer = AIOKafkaConsumer(
            KafkaTopics.klines_store_topic.value,
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_deserializer=lambda m: json.loads(m),
            group_id="klines_consumer",
        )
        await consumer.start()
        klines_provider = KlinesProvider(consumer)

        try:
            async for message in consumer:
                klines_provider.aggregate_data(message.value)
        finally:
            await consumer.stop()

    except UnknownMemberIdError:
        logging.error("UnknownMemberIdError in task_1, restarting consumer")
        await data_process_pipe()  # Restart the task
    except RequestTimedOutError:
        logging.error("RequestTimedOutError in task_1, restarting consumer")
        await asyncio.sleep(5)  # Add a delay before retrying
        await data_process_pipe()  # Restart the task
    except Exception as e:
        logging.error(f"Error in task_1: {e}")


async def data_analytics_pipe():
    try:
        consumer = AIOKafkaConsumer(
            KafkaTopics.signals.value,
            KafkaTopics.restart_streaming.value,
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_deserializer=lambda m: json.loads(m),
            session_timeout_ms=60000,  # Add session timeout
        )
        await consumer.start()
        telegram_consumer = TelegramConsumer(consumer)
        at_consumer = AutotradeConsumer(consumer)

        try:
            async for message in consumer:
                if message.topic == KafkaTopics.restart_streaming.value:
                    beginning_offsets = await consumer.beginning_offsets(
                        [message.partition]
                    )
                    offset = beginning_offsets.get(message.partition)
                    if message.offset == offset:
                        at_consumer.load_data_on_start()

                if message.topic == KafkaTopics.signals.value:
                    at_consumer.process_autotrade_restrictions(message.value)
                    telegram_consumer.send_telegram(message.value)
        finally:
            await consumer.stop()

    except UnknownMemberIdError:
        logging.error(
            "UnknownMemberIdError in data_analytics_pipe, restarting consumer"
        )
        await data_analytics_pipe()  # Restart the task
    except RequestTimedOutError:
        logging.error(
            "RequestTimedOutError in data_analytics_pipe, restarting consumer"
        )
        await asyncio.sleep(5)  # Add a delay before retrying
        await data_analytics_pipe()  # Restart the task
    except Exception as e:
        logging.error(f"Error in data_analytics_pipe: {e}")


async def main():
    await asyncio.gather(data_process_pipe(), data_analytics_pipe())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}")
        asyncio.run(main())

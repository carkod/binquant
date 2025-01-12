import asyncio
import json
import logging
import os
import concurrent.futures  # Add this import

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import RequestTimedOutError, UnknownMemberIdError

from consumers.autotrade_consumer import AutotradeConsumer
from consumers.klines_provider import KlinesProvider
from consumers.telegram_consumer import TelegramConsumer
from shared.enums import KafkaTopics

logging.basicConfig(
    level=logging.INFO,
    filename=None,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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
        )
        await consumer.start()
        telegram_consumer = TelegramConsumer()
        at_consumer = AutotradeConsumer(consumer)

        try:
            async for message in consumer:
                if message.topic == KafkaTopics.restart_streaming.value:
                    at_consumer.load_data_on_start()

                if message.topic == KafkaTopics.signals.value:
                    await telegram_consumer.send_msg(message.value)
                    at_consumer.process_autotrade_restrictions(message.value)
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
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await asyncio.gather(
            loop.run_in_executor(pool, asyncio.run, data_process_pipe()),
            loop.run_in_executor(pool, asyncio.run, data_analytics_pipe())
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}")
        asyncio.run(main())

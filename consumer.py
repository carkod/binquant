import json
import os
import asyncio
import time
import logging
from kafka import KafkaConsumer
from consumers.autotrade_consumer import AutotradeConsumer
from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider


def task_1():

    # Start consuming
    consumer = KafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
        api_version=(3, 4, 1)
    )

    klines_provider = KlinesProvider(consumer)
    try:
        for result in consumer:
            klines_provider.aggregate_data(result)
    except Exception as e:
        print("Error: ", e)
        task_1()
        pass
    finally:
        consumer.close()

def task_2():
    consumer = KafkaConsumer(
        KafkaTopics.signals.value,
        KafkaTopics.restart_streaming.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        value_deserializer=lambda m: json.loads(m),
    )

    telegram_consumer = TelegramConsumer(consumer)
    at_consumer = AutotradeConsumer(consumer)

    # Telegram flood control
    init_secs = time.time()
    message_count = 0

    try:
        for message in consumer:
            # Parse messages first
            # because it can be a restart or a signal
            # this is the only way because this consumer may be
            # too busy to process a separate topic, it never consumes
            if message.topic == KafkaTopics.restart_streaming.value:
                at_consumer.load_data_on_start()
            if message.topic == KafkaTopics.signals.value:
                at_consumer.process_autotrade_restrictions(message.value)
                if time.time() - init_secs > 1:
                    init_secs = time.time()
                else:
                    message_count += 1
                    if message_count > 20:
                        logging.warn("Telegram flood control")
                        return
                # If telegram is returning flood control error
                # probably this also overwhelms our server, so pause for both
                telegram_consumer.send_telegram(message.value)

    except Exception as e:
        print("Error: ", e)
        task_2()
        pass
    finally:
        consumer.close()


async def main():
    await asyncio.gather(asyncio.to_thread(task_1), asyncio.to_thread(task_2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        asyncio.run(main())
        pass

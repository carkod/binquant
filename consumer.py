import json
import os
import asyncio
import logging
from kafka import KafkaConsumer
from consumers.autotrade_consumer import AutotradeConsumer
from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider


def task_1():
    while True:
        try:
            # Start consuming
            consumer = KafkaConsumer(
                KafkaTopics.klines_store_topic.value,
                bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
                value_deserializer=lambda m: json.loads(m),
                group_id="klines_consumer",
                api_version=(3, 4, 1),
            )

            klines_provider = KlinesProvider(consumer)

            while True:
                messages = consumer.poll()
                if not messages:
                    continue
                for topic_partition, message_list in messages.items():
                    for message in message_list:
                        klines_provider.aggregate_data(message.value)

        except Exception as e:
            logging.error(f"Error in task_1: {e}")
        finally:
            consumer.close()


def task_2():
    while True:
        try:
            consumer = KafkaConsumer(
                KafkaTopics.signals.value,
                KafkaTopics.restart_streaming.value,
                bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
                value_deserializer=lambda m: json.loads(m),
            )

            telegram_consumer = TelegramConsumer(consumer)
            at_consumer = AutotradeConsumer(consumer)

            while True:
                messages = consumer.poll()

                if not messages:
                    continue

                for topic_partition, message_list in messages.items():
                    # Manage offsets to avoid repeated streams
                    beginning_offsets = consumer.beginning_offsets([topic_partition])
                    offset = beginning_offsets.get(topic_partition)

                    for message in message_list:
                        # Parse messages first
                        # because it can be a restart or a signal
                        # this is the only way because this consumer may be
                        # too busy to process a separate topic, it never consumes
                        if message.topic == KafkaTopics.restart_streaming.value:
                            # Avoid repeated loading
                            if message.offset == offset:
                                at_consumer.load_data_on_start()

                        if message.topic == KafkaTopics.signals.value:
                            telegram_consumer.send_telegram(message.value)
                            at_consumer.process_autotrade_restrictions(message.value)
                            pass

        except Exception as e:
            logging.error(f"Error in task_2: {e}")
        finally:
            consumer.close()


async def main():
    await asyncio.gather(asyncio.to_thread(task_1), asyncio.to_thread(task_2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}")
        asyncio.run(main())

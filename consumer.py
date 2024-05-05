import os
import asyncio
from consumers.autotrade_consumer import AutotradeConsumer
from shared.enums import KafkaTopics
from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider
from confluent_kafka import Consumer, KafkaException, KafkaError


def task_1():
    """
    Uses "at least once" delivery, to ensure
    we get all klines

    This is done: commit -> process data
    """

    # Start consuming
    consumer = Consumer({
        "bootstrap.servers": f'{os.environ["CONFLUENT_GCP_BOOTSTRAP_SERVER"]}:{os.environ["KAFKA_PORT"]}',
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': os.environ["CONFLUENT_API_KEY"],
        'sasl.password': os.environ["CONFLUENT_API_SECRET"],
        "group.id": "klines_consumer",
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 900000 # 15m interval
    })

    consumer.subscribe([KafkaTopics.klines_store_topic.value])
    klines_provider = KlinesProvider(consumer)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                data = message.value().decode()
                klines_provider.aggregate_data(data)

    except Exception as e:
        print("Error: ", e)
        consumer.close()
        task_1()
        pass
    finally:
        consumer.close()

def task_2():
    """
    Uses "at most once" delivery, to ensure
    we only get one message

    This is done: process data -> timeout -> commit
    The rationale behind is that because candlesticks are 15m,
    it will timeout when no data comes from websockets in the 15m interval
    """
    consumer = Consumer({
        "bootstrap.servers": f'{os.environ["CONFLUENT_GCP_BOOTSTRAP_SERVER"]}:{os.environ["KAFKA_PORT"]}',
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': os.environ["CONFLUENT_API_KEY"],
        'sasl.password': os.environ["CONFLUENT_API_SECRET"],
        "group.id": "long-live-messages",
        "max.poll.interval.ms": 900000
    })

    consumer.subscribe([KafkaTopics.signals.value, KafkaTopics.restart_streaming.value])
    telegram_consumer = TelegramConsumer(consumer)
    at_consumer = AutotradeConsumer(consumer)

    try:
        while True:
            message = consumer.poll(1)
            if message is None: continue
            if message.error():
                if message.error().code() == KafkaError._TIMED_OUT:
                    # At most once delivery
                    consumer.commit()
                else:
                    raise KafkaException(message.error())
            else:
                msg_topic = message.topic()
                # Parse messages first
                # because it can be a restart or a signal
                # this is the only way because this consumer may be
                # too busy to process a separate topic, it never consumes
                if msg_topic == KafkaTopics.restart_streaming.value:
                    at_consumer.load_data_on_start()
                if msg_topic == KafkaTopics.signals.value:
                    data = message.value().decode()
                    telegram_consumer.send_telegram(data)
                    at_consumer.process_autotrade_restrictions(data)            
                
    except Exception as e:
        print("Error: ", e)
        task_2()
        pass
    finally:
        consumer.close()

async def main():
    """
    Consumers are separted into 2 tasks because config is
    different for these 2 groups.

    Because while True is a long running task,
    they have to run in 2 threads to avoid blocking
    """
    await asyncio.gather(asyncio.to_thread(task_1), asyncio.to_thread(task_2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        asyncio.run(main())
        pass

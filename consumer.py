import os
import asyncio
import logging

from aiokafka import AIOKafkaConsumer
import json
from shared.enums import KafkaTopics
from consumers.klines_provider import KlinesProvider

async def main():

    # Create a consumer instance
    klines_consumer = AIOKafkaConsumer(
        KafkaTopics.klines_store_topic.value,
        bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        # group_id="klines",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m),
    )
    # Start consuming
    await klines_consumer.start()
    while True:
        tasks = []
        try:
        
            klines_provider = KlinesProvider(klines_consumer)
            async for result in klines_consumer:
                tasks.append(asyncio.create_task(klines_provider.aggregate_data(result)))

        except Exception as error:
            print(error)
        finally:
            await klines_consumer.stop()

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

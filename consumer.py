import os
import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from shared.enums import KafkaTopics
from consumers.klines_provider import KlinesProvider
from py4j.protocol import Py4JNetworkError

async def main():

    # Start consuming
    klines_provider = KlinesProvider()
    await klines_provider.consumer.start()
    while True:
        tasks = []
        try:
        
            set_1 = await klines_provider.get_future_tasks()
            tasks.extend(set_1)
            
        except Py4JNetworkError:
            asyncio.run(main())

        except Exception as error:
            print(error)
        finally:
            await klines_provider.consumer.stop()

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

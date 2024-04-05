import os
import asyncio
import logging

from consumers.telegram_consumer import TelegramConsumer
from consumers.klines_provider import KlinesProvider
from py4j.protocol import Py4JNetworkError

async def main():

    # Start consuming
    klines_provider = KlinesProvider()
    telegram_consumer = TelegramConsumer()
    await klines_provider.consumer.start()
    while True:
        try:
        
            future_1 = await klines_provider.get_future_tasks()
            future_2 = await telegram_consumer.get_future_tasks()
            
        except Py4JNetworkError:
            asyncio.run(main())

        except Exception as error:
            print(error)
        finally:
            await klines_provider.consumer.stop()

        await asyncio.gather([future_1, future_2])

if __name__ == "__main__":
    asyncio.run(main())

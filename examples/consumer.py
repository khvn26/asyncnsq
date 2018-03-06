import asyncio
import sys
import os
import logging
sys.path.append(os.getcwd())
from asyncnsq import create_nsq_consumer

logger = logging.getLogger()


def main():

    loop = asyncio.get_event_loop()

    async def go():
        try:
            nsq_consumer = await create_nsq_consumer(
                lookupd_http_addresses=[
                    ('127.0.0.1', 4161)],
                max_in_flight=200)

            await nsq_consumer.subscribe('test_async_nsq', 'nsq')

            for waiter in nsq_consumer.wait_messages():
                message = await waiter
                print(message.body)
                await message.fin()

            nsq_consumer = await create_nsq_consumer(
                host=['tcp://127.0.0.1:4150'],
                max_in_flight=200)

            await nsq_consumer.subscribe('test_async_nsq', 'nsq')

            for waiter in nsq_consumer.wait_messages():
                message = await waiter
                print(message.body)
                await message.fin()

        except Exception as tmp:
            logger.exception(tmp)

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()

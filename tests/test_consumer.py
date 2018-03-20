import asyncio
import sys

from asyncnsq.consumer import NsqConsumer
from asyncnsq.http import Nsqd
from asyncnsq.http.exceptions import NotFoundError
from asyncnsq.nsq import create_nsq

from ._testutils import BaseTest, run_until_complete


class NsqTest(BaseTest):
    def setUp(self):
        self.topic = 'foo'
        self.host = '127.0.0.1'
        self.port = 4150
        self.lookupd_port = 4161
        self.max_in_flight = 25
        self.total_test_msgs = 100
        self.test_range = range(1, self.total_test_msgs + 1)
        super().setUp()

    async def aioSetUp(self):
        self.producer = Nsqd(self.host, self.port+1, loop=self.loop)
        self.consumer = NsqConsumer(nsqd_tcp_addresses=[(self.host,
                                                         self.port)],
                                    max_in_flight=self.max_in_flight,
                                    loop=self.loop)

        for i in self.test_range:
            await self.producer.pub(self.topic, 'xxx:{}'.format(i))

        await asyncio.sleep(0.1, loop=self.loop)

        await self.consumer.connect()

    async def aioTearDown(self):
        await self.consumer.close()
        try:
            await self.producer.delete_topic(self.topic)
        except NotFoundError:
            pass
        finally:
            await self.producer.close()

    @run_until_complete
    async def test_consumer_waiters(self):
        await self.consumer.subscribe(self.topic, 'test_consumer')

        msgs = []

        for counter, waiter in enumerate(self.consumer.wait_messages(), 1):
            if self.consumer.is_starved():
                for msg in msgs:
                    self.assertIn(int(msg.text().split(':')[-1]),
                                  self.test_range)
                    await msg.fin()
                msgs = []

            if counter <= self.total_test_msgs:
                msg = await waiter
                msgs.append(msg)
                continue

            break

    @run_until_complete
    async def test_consumer_async_for(self):
        await self.consumer.subscribe(self.topic, 'test_consumer')

        if sys.version_info >= (3, 6):

            counter = 1

            async for msg in self.consumer.messages():
                if counter == self.total_test_msgs:
                    break

                counter += 1
                self.assertIn(int(msg.text().split(':')[-1]),
                              self.test_range)
                await msg.fin()

        else:
            with self.assertRaises(AttributeError):
                self.consumer.messages()

    @run_until_complete
    async def test_wait_for_topic(self):
        lookupd_addresses = [(self.host, self.lookupd_port)]
        self.consumer = NsqConsumer(lookupd_http_addresses=lookupd_addresses,
                                    max_in_flight=self.max_in_flight,
                                    lookupd_poll_interval=2,
                                    loop=self.loop)

        nonexistent_topic = 'moo'
        sub_coro = self.consumer.subscribe(nonexistent_topic, 'bar')
        sub_task = self.loop.create_task(sub_coro)

        await asyncio.sleep(1)

        await self.producer.pub(nonexistent_topic, 'test_msg')

        await sub_task

        msg = await self.consumer._queue.get()

        self.assertEqual('test_msg', msg.text())

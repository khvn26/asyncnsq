import asyncio

from asyncnsq.consumer import NsqConsumer
from asyncnsq.http import Nsqd
from asyncnsq.http.exceptions import NotFoundError
from asyncnsq.nsq import create_nsq

from ._testutils import BaseTest, run_until_complete


class NsqTest(BaseTest):
    def setUp(self):
        super().setUp()
        self.topic = 'foo'
        self.host = '127.0.0.1'
        self.port = 4150
        self.max_in_flight = 25
        self.total_test_msgs = 100

    async def aioTearDown(self):
        conn = Nsqd(self.host, self.port+1, loop=self.loop)
        try:
            await conn.delete_topic(self.topic)
        except NotFoundError:
            pass
        await conn.close()

    # @run_until_complete
    # def test_basic_instance(self):
    #     nsq = yield from create_nsq(host=self.host, port=self.port,
    #                                 heartbeat_interval=30000,
    #                                 feature_negotiation=True,
    #                                 tls_v1=True,
    #                                 snappy=False,
    #                                 deflate=False,
    #                                 deflate_level=0,
    #                                 loop=self.loop)
    #     yield from nsq.pub(b'foo', b'bar')
    #     yield from nsq.pub(b'foo', b'bar')
    #     yield from nsq.pub(b'foo', b'bar')
    #     yield from nsq.pub(b'foo', b'bar')
    #     yield from nsq.pub(b'foo', b'bar')
    #
    #     yield from nsq.sub(b'foo', b'bar')
    #     for i, waiter in enumerate(nsq.wait_messages()):
    #         # import ipdb; ipdb.set_trace()
    #         if i == 0:
    #             yield from nsq.rdy(3)
    #             # yield from nsq.rdy(1)
    #         message = yield from waiter
    #         yield from message.fin()
    #         break


    @run_until_complete
    async def test_consumer(self):
        nsq = await create_nsq(host=self.host, port=self.port,
                               heartbeat_interval=30000,
                               feature_negotiation=True,
                               tls_v1=True,
                               snappy=False,
                               deflate=False,
                               deflate_level=0,
                               loop=self.loop)

        for i in range(0, self.total_test_msgs):
            await nsq.pub(b'foo', 'xxx:{}'.format(i).encode('utf-8'))

        await asyncio.sleep(0.1, loop=self.loop)

        consumer = NsqConsumer(nsqd_tcp_addresses=[(self.host, self.port)],
                               max_in_flight=self.max_in_flight,
                               loop=self.loop)

        await consumer.connect()
        await consumer.subscribe(b'foo', b'bar')

        msgs = []

        for i, waiter in enumerate(consumer.wait_messages()):
            if i == self.total_test_msgs:
                break

            if consumer.is_starved():
                await asyncio.gather(*[m.fin() for m in msgs])
                msgs = []

            msg = await waiter
            msgs.append(msg)

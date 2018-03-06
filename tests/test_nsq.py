import asyncio

from asyncnsq.connection import NsqConnection, create_connection
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
        super().setUp()

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

        for i in range(0, 100):
            await nsq.pub(b'foo', 'xxx:{}'.format(i).encode('utf-8'))

        await asyncio.sleep(0.1, loop=self.loop)

        consumer = NsqConsumer(nsqd_tcp_addresses=[(self.host, self.port)],
                               max_in_flight=30, loop=self.loop)

        await consumer.connect()
        await consumer.subscribe(b'foo', b'bar')

        msgs = []

        for i, waiter in enumerate(consumer.wait_messages()):
            # yield from msg.fin()
            print('-----msgs', len(msgs))
            is_starved = consumer.is_starved()

            if is_starved:
                print(">>>>>>>>msgs in list: {}".format(len((msgs))))
                for m in msgs:
                    await m.fin()

                msgs = []
                break

            msg = await waiter
            msgs.append(msg)

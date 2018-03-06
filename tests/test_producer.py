from asyncnsq.producer import create_producer

from ._testutils import BaseTest, run_until_complete


class NsqTCPProducerTest(BaseTest):
    async def aioSetUp(self):
        endpoints = [('127.0.0.1', 4150)]
        config = {'tls_v1': False}

        self.nsq_producer = await create_producer(endpoints, config,
                                                  loop=self.loop)

    def tearDown(self):
        self.nsq_producer.close()
        super().tearDown()

    @run_until_complete
    async def test_publish(self):
        resp = await self.nsq_producer.publish('baz', 'producer msg')
        self.assertEqual(resp, b'OK')

    @run_until_complete
    async def test_mpublish(self):
        messages = ['baz:1', 'baz:2', 3.14, 42]
        resp = await self.nsq_producer.mpublish('baz', *messages)
        self.assertEqual(resp, b'OK')

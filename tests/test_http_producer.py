from asyncnsq.http.rest_producer import create_http_producer

from ._testutils import BaseTest, run_until_complete


class NsqHTTPProducerTest(BaseTest):
    async def aioSetUp(self):
        endpoints = [('127.0.0.1', 4151)]
        self.conn = await create_http_producer(endpoints,
                                               loop=self.loop)

    async def aioTearDown(self):
        await self.conn.close()

    @run_until_complete
    async def test_http_publish(self):
        resp = await self.conn.publish('http_baz', 'producer msg')
        self.assertEqual(resp, 'OK')

    @run_until_complete
    async def test_http_mpublish(self):
        messages = ['baz:1', b'baz:2', 3.14, 42]
        resp = await self.conn.mpublish('http_baz', *messages)
        self.assertEqual(resp, 'OK')

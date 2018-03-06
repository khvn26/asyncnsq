from asyncnsq.http.rest_producer import create_http_producer

from ._testutils import BaseTest, run_until_complete


class NsqHTTPProducerTest(BaseTest):
    @run_until_complete
    async def test_http_publish(self):

        endpoints = [('127.0.0.1', 4151)]
        nsq_producer = await create_http_producer(endpoints,
                                                  loop=self.loop)
        ok = await nsq_producer.publish('http_baz', 'producer msg')
        self.assertEqual(ok, 'OK')

    @run_until_complete
    async def test_http_mpublish(self):
        endpoints = [('127.0.0.1', 4151)]
        nsq_producer = await create_http_producer(endpoints,
                                                  loop=self.loop)
        messages = ['baz:1', b'baz:2', 3.14, 42]
        ok = await nsq_producer.mpublish('http_baz', *messages)
        self.assertEqual(ok, 'OK')

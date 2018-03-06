from asyncnsq.http.lookupd import NsqLookupd

from ._testutils import BaseTest, run_until_complete


class NsqLookupdTest(BaseTest):
    '''
    :see: http://nsq.io/components/nsqd.html
    '''
    def setUp(self):
        super().setUp()
        self.conn = NsqLookupd('127.0.0.1', 4161, loop=self.loop)

    async def aioTearDown(self):
        await self.conn.close()

    @run_until_complete
    async def test_ok(self):
        res = await self.conn.ping()
        self.assertEqual(res, 'OK')

    @run_until_complete
    async def test_info(self):
        res = await self.conn.info()
        self.assertTrue('version' in res)

    @run_until_complete
    async def test_lookup(self):
        res = await self.conn.lookup('foo')
        self.assertIn('producers', res)

    @run_until_complete
    async def test_topics(self):
        res = await self.conn.topics()
        self.assertIn('topics', res)

    @run_until_complete
    async def test_channels(self):
        res = await self.conn.channels('foo')
        self.assertIn('channels', res)

    @run_until_complete
    async def test_nodes(self):
        res = await self.conn.nodes()
        self.assertIn('producers', res)

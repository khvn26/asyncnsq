from asyncnsq.http.nsqd import Nsqd

from ._testutils import BaseTest, run_until_complete


class NsqdTest(BaseTest):
    '''
    :see: http://nsq.io/components/nsqd.html
    '''
    def setUp(self):
        super().setUp()
        self.conn = Nsqd('127.0.0.1', 4151, loop=self.loop)

    async def aioTearDown(self):
        await self.conn.close()

    @run_until_complete
    async def test_ok(self):
        res = await self.conn.ping()
        self.assertEqual(res, 'OK')

    @run_until_complete
    async def test_info(self):
        res = await self.conn.info()
        self.assertIn('version', res)

    @run_until_complete
    async def test_stats(self):
        res = await self.conn.stats()
        self.assertIn('version', res)

    @run_until_complete
    async def test_pub(self):
        res = await self.conn.pub('baz', 'baz_msg')
        self.assertEqual('OK', res)

    @run_until_complete
    async def test_mpub(self):
        res = await self.conn.mpub('baz', 'baz_msg_1', 'baz_msg_2')
        self.assertEqual('OK', res)

    @run_until_complete
    async def test_mpub_binary(self):
        res = await self.conn.mpub('baz', 'baz_msg_3', 'baz_msg_4', binary=True)
        self.assertEqual('OK', res)

    @run_until_complete
    async def test_crud_topic(self):
        res = await self.conn.create_topic('foo2')
        self.assertEqual('', res)
        res = await self.conn.delete_topic('foo2')
        self.assertEqual('', res)

    @run_until_complete
    async def test_crud_channel(self):
        res = await self.conn.create_topic('zap')
        self.assertEqual('', res)
        res = await self.conn.create_channel('zap', 'bar')
        self.assertEqual('', res)
        res = await self.conn.delete_channel('zap', 'bar')
        self.assertEqual('', res)
        res = await self.conn.delete_topic('zap')
        self.assertEqual('', res)

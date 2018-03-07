from asyncnsq.connection import NsqConnection, create_connection
from asyncnsq.http import Nsqd
from asyncnsq.http.exceptions import NotFoundError
from asyncnsq.protocol import DeflateReader, Reader, SnappyReader

from ._testutils import BaseTest, run_until_complete


class NsqConnectionTest(BaseTest):
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

    @run_until_complete
    async def test_basic_instance(self):
        host, port = '127.0.0.1', 4150
        nsq = await create_connection(host=host, port=port, loop=self.loop)

        self.assertIsInstance(nsq, NsqConnection)
        self.assertTrue('NsqConnection' in nsq.__repr__())
        self.assertTrue(not nsq.closed)
        self.assertTrue(host in nsq.endpoint)
        self.assertTrue(str(port) in nsq.endpoint)

        nsq.close()

        self.assertEqual(nsq.closed, True)

    @run_until_complete
    async def test_pub_sub(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        await self._pub_sub_rdy_fin(conn)

    @run_until_complete
    async def test_tls(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        config = {
            'feature_negotiation': True, 'tls_v1': True,
            'snappy': False, 'deflate': False
        }

        await conn.identify(**config)
        await self._pub_sub_rdy_fin(conn)

    @run_until_complete
    async def test_snappy(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        config = {
            'feature_negotiation': True, 'tls_v1': False,
            'snappy': True, 'deflate': False
        }
        self.assertIsInstance(conn._parser, Reader)

        await conn.identify(**config)
        
        self.assertIsInstance(conn._parser, SnappyReader)

        await self._pub_sub_rdy_fin(conn)

    @run_until_complete
    async def test_deflate(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        config = {
            'feature_negotiation': True, 'tls_v1': False,
            'snappy': False, 'deflate': True
        }
        self.assertIsInstance(conn._parser, Reader)

        await conn.identify(**config)
        self.assertIsInstance(conn._parser, DeflateReader)

        await self._pub_sub_rdy_fin(conn)

    async def _pub_sub_rdy_fin(self, conn):
        resp = await conn.execute(b'PUB', b'foo', data=b'msg foo')
        self.assertEqual(resp, b'OK')

        await conn.execute(b'SUB', b'foo', b'bar')
        await conn.execute(b'RDY', 1)

        msg = await conn._queue.get()
        self.assertEqual(msg.processed, False)

        await msg.fin()
        self.assertEqual(msg.processed, True)

        await conn.execute(b'CLS')
        conn.close()

    @run_until_complete
    async def test_message(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        resp = await conn.execute(b'PUB', self.topic, data=b'boom')
        self.assertEqual(resp, b'OK')

        await conn.execute(b'SUB', self.topic, b'bar')
        await conn.execute(b'RDY', 1)

        msg = await conn._queue.get()
        self.assertEqual(msg.processed, False)

        await msg.touch()
        self.assertEqual(msg.processed, False)

        await msg.req(1)
        self.assertEqual(msg.processed, True)

        await conn.execute(b'RDY', 1)
        new_msg = await conn._queue.get()

        await new_msg.fin()
        self.assertEqual(msg.processed, True)

        await conn.execute(b'CLS')
        conn.close()

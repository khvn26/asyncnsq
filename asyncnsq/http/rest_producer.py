import asyncio
from . import Nsqd
from ..selectors import RandomSelector
from ..producer import BaseNsqProducer


class NsqHTTPProducer(BaseNsqProducer):

    def __init__(self, nsqd_http_addresses, selector_factory=RandomSelector,
                 client_session=None, loop=None):
        self._endpoints = nsqd_http_addresses
        self._client_session = client_session
        self._loop = loop or getattr(client_session, '_loop', None) or asyncio.get_event_loop()
        self._selector = selector_factory()
        self._connections = {}

    def _get_connection(self):
        conn_list = list(self._connections.values())
        conn = self._selector.select(conn_list)
        return conn

    def connect(self):
        for endpoint in set(self._endpoints):
            if len(endpoint) == 2:
                conn = Nsqd(*endpoint, loop=self._loop, session=self._client_session)
            else:
                conn = Nsqd(base_url=endpoint, loop=self._loop, session=self._client_session)
            self._connections[conn.endpoint] = conn

    async def publish(self, topic, message):
        '''XXX

        :param topic:
        :param message:
        :return:
        '''
        conn = self._get_connection()
        return await conn.pub(topic, message)

    async def mpublish(self, topic, message, *messages):
        '''XXX

        :param topic:
        :param message:
        :param messages:
        :return:
        '''
        conn = self._get_connection()
        return await conn.mpub(topic, message, *messages)

    async def close(self):
        close_coros = [conn.close() for conn in self._connections.values()]
        await asyncio.gather(*close_coros)


async def create_http_producer(nsqd_http_addresses, selector_factory=RandomSelector,
                               loop=None):
    '''XXX

    :param nsqd_tcp_addresses:
    :param selector_factory:
    :param loop:
    :return:
    '''

    prod = NsqHTTPProducer(nsqd_http_addresses,
                           selector_factory=selector_factory, loop=loop)
    prod.connect()
    return prod

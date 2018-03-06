import asyncio
import random
from collections import deque
import time
import logging
from asyncnsq.http import NsqLookupd
from asyncnsq.http.exceptions import NsqHttpException
from asyncnsq.nsq import Nsq
from asyncnsq.utils import RdyControl

logger = logging.getLogger(__name__)


class NsqConsumer:
    """Experiment purposes"""

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, loop=None):

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []
        self._max_in_flight = max_in_flight

        self._loop = loop or asyncio.get_event_loop()

        self._queue = asyncio.Queue(loop=self._loop)

        self._connections = {}

        self._idle_timeout = 10

        self._max_in_flight = max_in_flight

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)

    async def _connect(self, *addresses):
        addresses = addresses or self._nsqd_tcp_addresses

        if not addresses:
            return

        conn_coros = []

        for host, port in addresses:
            conn = Nsq(host, port, loop=self._loop, queue=self._queue)
            conn_coros.append(conn.connect())
            self._connections[conn.id] = conn

        await asyncio.gather(*conn_coros)

        self._rdy_control.add_connections(self._connections)

    async def connect(self):
        await self._connect()

    async def _poll_lookupd(self, *addresses):
        if not self.topic:
            return

        addresses = addresses or self._lookupd_http_addresses

        if not addresses:
            return

        host, port = random.choice(addresses)

        nsqd_addresses = []

        nsqlookup_conn = NsqLookupd(host, port, loop=self._loop)

        try:
            res = await nsqlookup_conn.lookup(self.topic)
            logger.debug('lookupd response')
            logger.debug(res)

        except NsqHttpException as exc:
            logger.error(exc)
            logger.exception(exc, exc_info=1)

        for producer in res['producers']:
            # host, port = producer['127.0.0.1'], producer['tcp_port']
            # producer['broadcast_address']

            host = producer.get('broadcast_address',
                                producer.get('address'))

            port = producer['tcp_port']

            tmp_id = 'tcp://{}:{}'.format(host, port)
            if tmp_id not in self._connections:
                nsqd_addresses.append((host, port))

        await self._connect(*nsqd_addresses)
        await nsqlookup_conn.close()

    # async def lookupd_task_done_sub(self, topic, channel):
    #     for conn in self._connections.values():
    #         result = await conn.sub(topic, channel)
    #     self._redistribute_task = asyncio.Task(self._redistribute(),
    #                                            loop=self._loop)

    async def subscribe(self, topic, channel):
        self.topic = topic

        if self._lookupd_http_addresses:
            await self._poll_lookupd()

        self._is_subscribe = True

        sub_coros = (conn.sub(topic, channel)
                     for conn in self._connections.values())

        await asyncio.gather(*sub_coros)

        self._redistribute_task = self._loop.create_task(self._redistribute())

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            fut = self._loop.create_task(self._queue.get())
            yield fut

    async def messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            yield await self._queue.get()

    def is_starved(self):
        return any(conn.is_starved()
                   for conn in self._connections.values())

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout,
                                loop=self._loop)

    # async def _lookupd(self):
    #     host, port = random.choice(self._lookupd_http_addresses)
    #     result = await self._poll_lookupd(host, port)

    async def close(self):
        cls_coros = []

        for conn in self._connections.values():
            cls_coros.append(conn.cls())

        try:
            await asyncio.gather(*cls_coros)

        except Exception as exc:
            logger.error(exc, exc_info=1)

        finally:
            self._rdy_control.remove_all()
            self._is_subscribe = False

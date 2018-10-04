import asyncio
import logging
import random

from asyncnsq.http import NsqLookupd
from asyncnsq.http.exceptions import NsqHttpException, NotFoundError
from asyncnsq.nsq import Nsq
from asyncnsq.utils import PY_36, RdyControl

logger = logging.getLogger(__name__)


class NsqConsumer:
    '''Experiment purposes'''

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, lookupd_poll_interval=5, client_session=None, loop=None):

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []
        self._max_in_flight = max_in_flight

        self._loop = loop or getattr(client_session, "_loop", None) \
                          or asyncio.get_event_loop()

        self._queue = asyncio.Queue(loop=self._loop)

        self._connections = {}

        self._idle_timeout = 10

        self._max_in_flight = max_in_flight

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_interval = lookupd_poll_interval  # sec
        self._client_session = client_session
        self.topic = None
        self._redistribute_task = None
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

            if conn.id in self._connections:
                del conn
                continue

            conn_coros.append(conn.connect())
            self._connections[conn.id] = conn

        await asyncio.gather(*conn_coros)

        self._rdy_control.add_connections(self._connections)

    async def connect(self):
        await self._connect()

    async def _poll_lookupd(self, endpoint, topic, poll_interval=0):
        nsqd_addresses = set()
        poll_interval = poll_interval or self._lookupd_poll_interval

        args = ()
        kwargs = {
            'session': self._client_session,
            'loop': self._loop,
        }

        if len(endpoint) == 2:
            args = endpoint

        else:
            kwargs['base_url'] = endpoint

        nsqlookup_conn = NsqLookupd(*args, **kwargs)

        resp = None

        while resp is None:
            try:
                resp = await nsqlookup_conn.lookup(topic)

            except NotFoundError:
                logger.debug(('topic not registered in lookupd. '
                              'retrying in %d s...'), poll_interval)
                await asyncio.sleep(poll_interval)
                continue

            except NsqHttpException as exc:
                logger.error(exc)
                logger.exception(exc, exc_info=1)
                raise exc

            else:
                logger.debug('lookupd response')
                logger.debug(resp)

                assert 'producers' in resp and isinstance(resp['producers'],
                                                          list)

        for producer in resp['producers']:
            # host, port = producer['127.0.0.1'], producer['tcp_port']
            # producer['broadcast_address']

            host = producer.get('broadcast_address',
                                producer.get('address'))

            port = producer['tcp_port']

            nsqd_addresses.add((host, port))

        await nsqlookup_conn.close()
        return nsqd_addresses

    async def _connect_via_lookupd(self, *addresses, topic):
        addresses = addresses or self._lookupd_http_addresses
        endpoint = random.choice(addresses)
        nsqd_addresses = await self._poll_lookupd(endpoint, topic)
        await self._connect(*nsqd_addresses)

    async def subscribe(self, topic, channel):
        self.topic = topic

        if self._lookupd_http_addresses:
            await self._connect_via_lookupd(topic=topic)

        sub_coros = (conn.sub(topic, channel)
                     for conn in self._connections.values())

        await asyncio.gather(*sub_coros)

        self.topic, self._is_subscribe = topic, True
        self._redistribute_task = self._loop.create_task(self._redistribute())

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            fut = self._loop.create_task(self._queue.get())
            yield fut

    if PY_36:
        from .consumer_async_iter import messages
        messages = messages

    def is_starved(self):
        return any(conn.is_starved()
                   for conn in self._connections.values())

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout,
                                loop=self._loop)

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

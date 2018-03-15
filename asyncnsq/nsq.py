import asyncio

from . import consts
from .connection import NsqConnection
from .consts import AUTH, CLS, DPUB, FIN, MPUB, PUB, RDY, REQ, SUB, TOUCH
from .log import logger
from .utils import retry_iterator


async def create_nsq(host='127.0.0.1', port=4150, loop=None, queue=None,
                     heartbeat_interval=30000, feature_negotiation=True,
                     tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                     sample_rate=0):
    '''"
    param: host: host addr with no protocol. 127.0.0.1 
    param: port: host port 
    param: queue: queue where all the msg been put from the nsq 
    param: heartbeat_interval: heartbeat interval with nsq, set -1 to disable nsq heartbeat check
    params: snappy: snappy compress
    params: deflate: deflate compress  can't set True both with snappy
    '''
    # TODO: add parameters type and value validation
    loop = loop or asyncio.get_event_loop()
    queue = queue or asyncio.Queue(loop=loop)
    conn = Nsq(host=host, port=port, queue=queue,
               heartbeat_interval=heartbeat_interval,
               feature_negotiation=feature_negotiation,
               tls_v1=tls_v1, snappy=snappy, deflate=deflate,
               deflate_level=deflate_level,
               sample_rate=sample_rate, loop=loop)
    await conn.connect()
    return conn


class Nsq:

    def __init__(self, host='127.0.0.1', port=4150, loop=None, queue=None,
                 heartbeat_interval=30000, feature_negotiation=True,
                 tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                 sample_rate=0):
        # TODO: add parameters type and value validation
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }

        self._host = host
        self._port = port
        self._loop = loop
        self._queue = queue or asyncio.Queue(loop=self._loop)

        self._status = consts.INIT
        self._reconnect = True
        self._rdy_state = 0
        self._last_message = None

        self._rdy_callback = None

        self._last_rdy = 0
        self._is_subscribe = False

        self._conn = NsqConnection(self._host, self._port,
                                   queue=self._queue, loop=self._loop,
                                   on_message=self._on_message)

    def set_rdy_callback(self, callback):
        self._rdy_callback = callback

    async def connect(self):
        await self._conn.connect()

        await self._conn.identify(**self._config)

        self._status = consts.CONNECTED

    def _on_message(self, msg):
        # should not be coroutine
        # update connections rdy state
        self.rdy_state = int(self.rdy_state) - 1

        # self._last_message = time.time()
        self._last_message = msg.timestamp * 1e-9

        if self._rdy_callback is not None:
            self._rdy_callback(self.id)

        return msg

    @property
    def rdy_state(self):
        return self._rdy_state

    @rdy_state.setter
    def rdy_state(self, value):
        self._rdy_state = value

    @property
    def in_flight(self):
        return self._conn.in_flight

    @property
    def last_message(self):
        return self._last_message

    async def reconnect(self):
        timeout_generator = retry_iterator(init_delay=0.1, max_delay=10.0)

        while self._status != consts.CONNECTED:
            try:
                await self.connect()

            except ConnectionError:
                logger.error("reconnect failed: %s:%d ",
                             self._host, self._port)

            else:
                self._status = consts.CONNECTED

            timeout = next(timeout_generator)

            await asyncio.sleep(timeout, loop=self._loop)

    async def execute(self, command, *args, data=None):
        if self._status <= consts.CONNECTED:
            await self.reconnect()

        return await self._conn.execute(command, *args, data=data)

    @property
    def id(self):
        return self._conn.endpoint

    async def auth(self, secret):
        '''

        :param secret:
        :return:
        '''
        return await self.execute(AUTH, data=secret)

    async def sub(self, topic, channel):
        '''

        :param topic:
        :param channel:
        :return:
        '''
        self._is_subscribe = True

        return await self.execute(SUB, topic, channel)

    # async def _redistribute(self):
    #     while self._is_subscribe:
    #         self._rdy_control.redistribute()
    #         await asyncio.sleep(60, loop=self._loop)

    async def pub(self, topic, message):
        '''

        :param topic:
        :param message:
        :return:
        '''
        return await self.execute(PUB, topic, data=message)

    async def dpub(self, topic, delay_time, message):
        '''

        :param topic:
        :param message:
        :param delay_time: delayed time in millisecond
        :return:
        '''
        if not delay_time or delay_time is None:
            delay_time = 0
        return await self.execute(DPUB, topic, delay_time, data=message)

    async def mpub(self, topic, message, *messages):
        '''

        :param topic:
        :param message:
        :param messages:
        :return:
        '''
        msgs = [message] + list(messages)
        return await self.execute(MPUB, topic, data=msgs)

    async def rdy(self, count):
        '''

        :param count:
        :return:
        '''
        if not isinstance(count, int):
            raise TypeError('count argument must be int')

        self._last_rdy = count
        self.rdy_state = count
        return await self.execute(RDY, count)

    async def fin(self, message_id):
        '''

        :param message_id:
        :return:
        '''
        return await self.execute(FIN, message_id)

    async def req(self, message_id, timeout):
        '''

        :param message_id:
        :param timeout:
        :return:
        '''
        return await self.execute(REQ, message_id, timeout)

    async def touch(self, message_id):
        '''

        :param message_id:
        :return:
        '''
        return await self.execute(TOUCH, message_id)

    async def cls(self):
        '''

        :return:
        '''
        await self.execute(CLS)
        self.close()

    def close(self):
        self._conn.close()
        self._status = consts.CLOSED

    def is_starved(self):

        if self._queue.qsize():
            starved = False
        else:
            starved = (self.in_flight > 0 and
                       self.in_flight >= (self._last_rdy * 0.85))
        return starved

    @property
    def last_rdy(self):
        return self._last_rdy

    def __repr__(self):
        return '<Nsq conn={} >'.format(self._conn.__repr__())

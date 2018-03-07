import asyncio

import ssl
from collections import deque

from . import consts
from ._json import json
from .containers import NsqMessage
from .exceptions import ProtocolError, ERROR_CODES
from .log import logger
from .protocol import DeflateReader, Reader, SnappyReader


async def create_connection(host='localhost', port=4151, queue=None,
                            loop=None, on_message=None):
    '''XXX'''
    conn = NsqConnection(host, port, queue=queue,
                         loop=loop, on_message=on_message)

    await conn.connect()

    return conn


class NsqConnection:
    '''XXX'''

    def __init__(self, host, port, *, reader=None, writer=None,
                 on_message=None, queue=None, loop=None):
        # assign parameter values
        self._reader, self._writer = reader, writer
        self._host, self._port = host, port
        self._on_message = on_message
        self._loop = loop or asyncio.get_event_loop()

        assert isinstance(queue, (asyncio.Queue, type(None)))
        self._queue = queue or asyncio.Queue(loop=self._loop)

        # set up attributes
        ## default parser
        self._parser = Reader()
        ## NSQ command execution queue
        self._cmd_waiters = deque()

        ## attribute for _read_data task storage
        self._reader_task = None
        ## indicates whether the connection is in upgrading state
        self._is_upgrading = False
        ## indicates whether the connection is closed
        self._closed = False
        ## number of received but not acked or req messages
        self._in_flight = 0
        ## whether this connection uses TLS
        self._tls = False

    # Private methods below.

    async def _establish_connection(self, *, 
                                    tls=None,
                                    limit=consts.MAX_CHUNK_SIZE):
        tls = tls or self._tls

        transport = None
        kwargs = {
            'host': self._host,
            'port': self._port
        }

        if self._writer:
            transport = self._writer.transport
            transport.pause_reading()

            sock = transport.get_extra_info('socket')
            if sock:
                kwargs = {'sock': sock}

        if not self._reader:
            self._reader = asyncio.StreamReader(limit=limit,
                                                loop=self._loop)

        protocol = asyncio.StreamReaderProtocol(self._reader,
                                                loop=self._loop)

        if tls:
            kwargs.update({
                'server_hostname': self._host,
                'ssl': ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            })

            # if not kwargs.get('sock'):
            #     raise RuntimeError('transport does not provide a raw socket')

        transport, _ = await self._loop.create_connection(lambda: protocol,
                                                          **kwargs)

        self._writer = asyncio.StreamWriter(transport, protocol,
                                            self._reader, self._loop)

    def _do_close(self, exc=None):
        if exc:
            logger.error('connection closed with error: %s', exc, exc_info=1)

        if self._closed:
            return

        self._closed = True
        self._writer.transport.close()
        self._reader_task.cancel()

    async def _upgrade_to_tls(self):
        self._tls = True
        self._reader_task.cancel()

        await self._establish_connection()

        response = await self._reader.readexactly(10)
        if response != consts.BIN_OK:
            raise RuntimeError('upgrade to TLS failed, received: ' +
                               str(response))

        self._reader_task = self._loop.create_task(self._read_data)
        # self._reader_task.add_done_callback(self._on_reader_task_stopped)

    def _change_parser_cls(self, cls):
        self._parser = cls(self._parser.buffer)

        future = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((future, None))

        return future

    # def _on_reader_task_stopped(self, future):
    #     exc = future.exception()
    #     logger.error('DONE: TASK %s', exc)

    async def _read_data(self):
        ''' Response reader loop.
        '''
        is_cancelled = False

        while not self._reader.at_eof():
            try:
                data = await self._reader.read(52)

            except asyncio.CancelledError:
                is_cancelled = True
                logger.debug('reader task was cancelled')
                break

            except Exception as exc:
                logger.exception(exc)
                logger.debug('reader task stopped due to: %s',
                             exc, exc_info=1)
                break

            self._parser.feed(data)

            if not self._is_upgrading:
                self._read_buffer()

        if is_cancelled:
            # useful during update to TLS, task canceled but connection
            # should not be closed
            return

        # self._closing = True
        self.close()

    def _parse_data(self):
        try:
            obj = self._parser.gets()

        except ProtocolError as exc:
            # ProtocolError is fatal, connection must be closed
            logger.exception(exc)

            # self._closing = True
            self.close(exc)
            return False

        if obj is False:
            return obj

        logger.debug('got nsq data: %s', obj)

        resp_type, resp = obj

        if resp_type == consts.FRAME_TYPE_MESSAGE:
            # track number in flight messages
            self._in_flight += 1

            timestamp, attempts, message_id, body = resp

            self._on_message_hook(timestamp, attempts, message_id, body)

        if resp_type == consts.FRAME_TYPE_ERROR:
            code, message = resp

            logger.error('nsqd error: %s, %s', code, message)

            error = ERROR_CODES[code]

            if error.fatal:
                raise error

        if resp_type == consts.FRAME_TYPE_RESPONSE:
            if resp == consts.HEARTBEAT:
                self._execute(consts.NOP)

            else:
                future, callback = self._cmd_waiters.popleft()

                if not future.cancelled():
                    future.set_result(resp)

                    if callback is not None:
                        callback(resp)

        return True

    def _execute(self, command, *args, data=None, encode=True):
        if encode:
            command = self._parser.encode_command(command, *args, data=data)

        logger.debug('execute command %s', command)
        self._writer.write(command)

    def _on_message_hook(self, timestamp, attempts, message_id, body):
        msg = NsqMessage(timestamp, attempts, message_id, body, self)

        if self._on_message is not None:
            msg = self._on_message(msg)

        self._queue.put_nowait(msg)

    def _read_buffer(self):
        is_continue = True
        while is_continue:
            is_continue = self._parse_data()

    def _start_upgrading(self, *_):
        self._is_upgrading = True

    def _finish_upgrading(self, *_):
        self._read_buffer()
        self._is_upgrading = False

    def __repr__(self):
        return '<NsqConnection: {}:{}>'.format(self._host, self._port)

    # Public methods below.

    async def connect(self):
        ''' Establish a connection with `nsqd`: set up streams, launch
        a reader loop, send the magic packet.

        This is the first method you call do after creating the connection
        instance.
        '''
        if not (self._reader and self._writer):
            await self._establish_connection()

        # start reader loop
        self._reader_task = self._loop.create_task(self._read_data())

        # send magic packet
        self._execute(consts.MAGIC_V2, encode=False)

    async def identify(self, **config):
        ''' Negotiate features with `nsqd` and tune the connection
        accordingly.

        Args:
            **config: local configuration
        '''
        # TODO: add config validator
        resp = await self.execute(consts.IDENTIFY,
                                  data=json.dumps(config))

        future = None

        if resp != consts.OK:
            self._start_upgrading()

            resp_config = json.loads(resp.decode('utf-8'))

            if resp_config.get('tls_v1'):
                await self._upgrade_to_tls()

            if resp_config.get('snappy'):
                future = self._change_parser_cls(SnappyReader)

            elif resp_config.get('deflate'):
                future = self._change_parser_cls(DeflateReader)

            self._finish_upgrading()

        if future:
            assert await future == consts.OK

        return resp

    def execute(self, command: bytes, *args, data=None, callback=None):
        ''' Execute a NSQ command, schedule callback if provided.

        Args:
            command: a NSQ command
            *args: command arguments
            data: (optional) request body
            callback: (optional) callback function to pass the command
                result to
        '''
        assert not self.closed, 'connection closed or corrupted'

        if command is None:
            raise ValueError('command must not be None')

        if None in set(args):
            raise ValueError('args must not contain None')

        future = asyncio.Future(loop=self._loop)

        if command in (consts.NOP, consts.FIN, consts.RDY,
                       consts.REQ, consts.TOUCH):
            future.set_result(consts.OK)

        else:
            self._cmd_waiters.append((future, callback))

        self._execute(command, *args, data=data)

        # track all processed and requeued messages
        # if command in (b'FIN', b'REQ', 'FIN', 'REQ'):
        if command in (consts.FIN, consts.REQ):
            self._in_flight = max(0, self._in_flight - 1)

        return future

    @property
    def in_flight(self):
        ''' Count of messages currently in flight.
        '''
        return self._in_flight

    @property
    def endpoint(self):
        ''' Full `nsqd` address.
        '''
        return "tcp://{}:{}".format(self._host, self._port)

    @property
    def closed(self):
        ''' True if connection is closed.
        '''
        if not self._closed and self._reader and self._reader.at_eof():
            self.close()

        return self._closed

    @property
    def queue(self):
        ''' Queue used to store received messages.
        '''
        return self._queue

    def close(self, exc=None):
        ''' Close the connection.
        '''
        # self._loop.call_soon(self._do_close, exc)
        self._do_close(exc)

from collections import namedtuple

from ._json import json
from .consts import FIN, REQ, TOUCH

__all__ = [
    'NsqMessage',
    # 'NsqErrorMessage'
]


_NSQ_MESSAGE_FIELDS = ('timestamp', 'attempts', 'message_id', 'body', 'conn')
# NSQ_ERROR_MESSAGE_FIELDS = ('code', 'msg')

# class NsqErrorMessage(namedtuple('_', NSQ_ERROR_MESSAGE_FIELDS)):
#     pass


class NsqMessage(namedtuple('_', _NSQ_MESSAGE_FIELDS)):
    ''' Represents a NSQ message.
    Not meant to be used directly.

    Attributes:
        timestamp: (int) nanosecond timestamp
        attempts: (int) attempts count
        message_id: (int) message id
        body: (bytes) message body
        conn: (NsqConnection) a connection which received the message
    '''
    def __init__(self, *_):
        super().__init__()
        self._is_processed = False

    # Private methods below.

    async def _execute(self, command: bytes, *args):
        return await self.conn.execute(command, self.message_id, *args)

    async def _request_and_process(self, command: bytes, *args):
        if self._is_processed:
            raise RuntimeWarning('message has already been processed')

        resp = await self._execute(command, *args)
        self._is_processed = True
        return resp

    def __repr__(self):
        return '<{} id={}>'.format(self.__class__.__name__, self.message_id)

    # Public methods below.

    @property
    def processed(self):
        ''' True if message has been processed: finished or re-queued.
        '''
        return self._is_processed

    def text(self) -> str:
        ''' Message body, decoded.
        '''
        return self.body.decode('utf-8')

    def json(self, loads=json.loads) -> dict:
        ''' Message body, decoded from JSON.

        Args:
            loads: (optional) decoder function

        Raises:
            ValueError: from JSON decoder.
        '''
        return loads(self.text())

    async def fin(self):
        ''' Finish a message (indicate successful processing).

        Raises:
            RuntimeWarning: in case message was processed earlier.
        '''
        return await self._request_and_process(FIN)

    async def req(self, timeout: int = 10):
        ''' Re-queue a message (indicate failure to process).

        Args:
            timeout: configured max timeout 0 is a special case
                that will not defer re-queueing.

        Raises:
            RuntimeWarning: in case message was processed earlier.
        '''
        return await self._request_and_process(REQ, timeout)

    async def touch(self):
        ''' Reset the timeout for an in-flight message.

        Raises:
            RuntimeWarning: in case message was processed earlier.
        '''
        return await self._execute(TOUCH)

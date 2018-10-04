import asyncio
import logging

import aiohttp
from yarl import URL

from .._json import json
from .exceptions import HTTP_EXCEPTIONS


logger = logging.getLogger(__name__)


class NsqHTTPConnection:
    ''' Base class for HTTP connections to nsqd and lookupd.

    Args:
        host: service host.
        port: service port.
        loop: event loop instance.
        session: aiohttp.ClientSession instance.
    '''
    def __init__(self, base_url, *, loop=None, session=None):
        self._loop = loop or asyncio.get_event_loop()

        if session is not None:
            self._session = session
            self._own_session = False

        else:
            self._session = aiohttp.ClientSession(loop=self._loop,
                                                  json_serialize=json.dumps)
            self._own_session = True

        assert self._loop is self._session.loop, \
               'loop and session loop should not be different'

        self.endpoint = base_url if isinstance(base_url, URL) else URL(base_url)

    # Private methods below.

    @staticmethod
    async def _parse_response(response: aiohttp.ClientResponse):
        try:
            return await response.json(loads=json.loads)
        except aiohttp.ContentTypeError:
            return await response.text()

    @classmethod
    async def _raise_for_status(cls, response: aiohttp.ClientResponse):
        if not 200 <= response.status <= 300:
            data = await cls._parse_response(response)

            if isinstance(data, dict):
                msg, extra = '', data
            else:
                msg, extra = data, None

            exc_cls = HTTP_EXCEPTIONS[response.status]

            raise exc_cls(response.status, msg, extra)

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.endpoint)

    # Public methods below.

    async def close(self):
        ''' Close the connection.

        Raises:
            RuntimeWarning: in case a session was provided for connection,
                not created with it
        '''
        if not self._own_session:
            logger.warning('tried to close a session provided from '
                           'outside, which is probably not a good idea')
            return

        return await self._session.close()

    async def perform_request(self, method, path, params=None, body=None):
        ''' Perform an HTTP request.

        Args:
            method: an HTTP method.
            path: relative path to a resource.
            params: (optional) query params.
            body: (optional) request body.
        '''
        url = self.endpoint.with_path(path)

        kwargs = {
            'params': params
        }

        if isinstance(body, (dict, list)):
            kwargs['json'] = body

        elif isinstance(body, str):
            kwargs['data'] = body.encode('utf-8')

        elif isinstance(body, bytes):
            kwargs['data'] = body

        resp = await self._session.request(method, url, **kwargs)

        await self._raise_for_status(resp)

        return await self._parse_response(resp)

from yarl import URL

from .connection import NsqHTTPConnection
from ..consts import DEFAULT_HOST, DEFAULT_NSQLOOKUPD_PORT_HTTP


class NsqLookupd(NsqHTTPConnection):
    ''' `nsqlookupd` HTTP protocol implementation.

    Full reference: 'http://nsq.io/components/nsqlookupd.html'
    '''
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_NSQLOOKUPD_PORT_HTTP,
                 *, base_url=None, loop=None, session=None, secure=False):
        base_url = base_url or URL.build(
            scheme='https' if secure else 'http',
            host=host,
            port=port,
        )
        super().__init__(base_url, loop=loop, session=session)

    # Public methods below.

    async def ping(self):
        ''' Monitoring endpoint, should return 200 OK.
        Raises NsqHttpException if it is not healthy.
        '''
        return await self.perform_request('GET', 'ping')

    async def info(self):
        ''' Version information.
        '''
        return await self.perform_request('GET', 'info')

    async def lookup(self, topic: str):
        ''' Returns a list of producers for a topic.

        Args:
            topic: the topic to list producers for
        '''
        return await self.perform_request('GET', 'lookup', {'topic': topic})

    async def topics(self):
        ''' Returns a list of all known topics.
        '''
        return await self.perform_request('GET', 'topics')

    async def channels(self, topic: str):
        ''' Returns a list of all known channels of a topic.

        Args:
            topic: the topic to list channels for
        '''
        return await self.perform_request('GET', 'channels', {'topic': topic})

    async def nodes(self):
        ''' Returns a list of all known `nsqd`.
        '''
        return await self.perform_request('GET', 'nodes')

    async def create_topic(self, topic: str):
        ''' Add a topic to `nsqlookupd`’s registry.

        Args:
            topic: name of topic
        '''
        return await self.perform_request('POST', 'topic/create',
                                          {'topic': topic})

    async def delete_topic(self, topic: str):
        ''' Deletes an existing topic.

        Args:
            topic: the existing topic to delete
        '''
        return await self.perform_request('POST', 'topic/delete',
                                          {'topic': topic})

    async def create_channel(self, topic: str, channel: str):
        ''' Add a channe; to `nsqlookupd`’s registry.

        Args:
            topic: name of topic
            channel: name of channel
        '''
        return await self.perform_request('POST', 'channel/create',
                                          {'topic': topic,
                                           'channel': channel})

    async def delete_channel(self, topic: str, channel: str):
        ''' Deletes an existing channel of an existing topic.

        Args:
            topic: the existing topic
            channel: the existing channel to delete
        '''
        return await self.perform_request('POST', 'channel/delete',
                                          {'topic': topic,
                                           'channel': channel})

    async def topic_tombstone(self, topic: str, node: str):
        ''' Tombstones a specific producer of an existing topic.

        Args:
            topic: the existing topic
            node: the producer (nsqd) to tombstone (identified by
                <broadcast_address>:<http_port>)
        '''
        return await self.perform_request('POST', 'topic/tombstone',
                                          {'topic': topic, 'node': node})

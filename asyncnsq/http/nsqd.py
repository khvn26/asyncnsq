from .connection import NsqHTTPConnection
from ..consts import DEFAULT_HOST, DEFAULT_NSQD_PORT_HTTP
from ..utils import encode_msgs


class Nsqd(NsqHTTPConnection):
    ''' `nsqd` HTTP protocol implementation.

    Full reference: 'http://nsq.io/components/nsqd.html'
    '''
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_NSQD_PORT_HTTP, *,
                 loop=None, session=None):
        super().__init__(host, port, loop=loop, session=session)

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

    async def stats(self, **kwargs):
        ''' Return internal statistics.

        Args:
            topic: (optional) filter to topic
            channel: (optional) filter to channel
        '''
        params = {'format': 'json'}

        for key in ('topic', 'channel'):
            value = kwargs.pop(key, None)
            if value:
                params[key] = value

        return await self.perform_request('GET', 'stats', params)

    async def pub(self, topic: str, message: str, defer: int = None):
        ''' Publish a message.

        Args:
            topic: the topic to publish to
            defer: (optional) the time in ms to delay message
                delivery (optional)
        '''
        params = {'topic': topic}

        if defer:
            params['defer'] = defer

        return await self.perform_request('POST', 'pub', params, message)

    async def mpub(self, topic: str, *messages, binary=False):
        ''' Publish multiple messages in one roundtrip.

        Args:
            topic: the topic to publish to
            *messages: messages to send
            binary: if True, enable binary mode (for newlines
                inside messages)
        '''
        if not messages:
            raise ValueError('no messages provided')

        params = {'topic': topic}

        if binary:
            params['binary'] = 'true'
            data = encode_msgs(*messages)

        else:
            data = '\n'.join('{}'.format(m) for m in messages)

        return await self.perform_request('POST', 'mpub', params, data)

    async def create_topic(self, topic: str):
        ''' Create a topic

        Args:
            topic: the topic to create
        '''
        return await self.perform_request('POST', 'topic/create',
                                          {'topic': topic})

    async def delete_topic(self, topic: str):
        ''' Delete an existing topic (and all channels).

        Args:
            topic: the existing topic to delete
        '''
        return await self.perform_request('POST', 'topic/delete',
                                          {'topic': topic})

    async def create_channel(self, topic: str, channel: str):
        ''' Create a channel for an existing topic.

        Args:
            topic: the existing topic
            channel: the channel to create
        '''
        return await self.perform_request('POST', 'channel/create',
                                          {'topic': topic,
                                           'channel': channel})

    async def delete_channel(self, topic: str, channel: str):
        ''' Delete an existing channel on an existing topic.

        Args:
            topic: the existing topic
            channel: the existing channel to delete
        '''
        return await self.perform_request('POST', 'channel/delete',
                                          {'topic': topic,
                                           'channel': channel})

    async def empty_topic(self, topic: str):
        ''' Empty all the queued messages (in-memory and disk)
        for an existing topic.

        Args:
            topic: the existing topic to empty
        '''
        return await self.perform_request('POST', 'topic/empty',
                                          {'topic': topic})

    async def empty_channel(self, channel: str):
        ''' Empty all the queued messages (in-memory and disk)
        for an existing channel.

        Args:
            topic: the existing topic to empty
        '''
        return await self.perform_request('POST', 'channel/empty',
                                          {'channel': channel})

    async def pause_topic(self, topic: str):
        ''' Pause message flow to all channels on an existing topic
        (messages will queue at topic).

        Args:
            topic: the existing topic
        '''
        return await self.perform_request('POST', 'topic/pause',
                                          {'topic': topic})

    async def unpause_topic(self, topic: str):
        ''' Resume message flow to channels of an existing, paused, topic.

        Args:
            topic: the existing topic
        '''
        return await self.perform_request('POST', 'topic/unpause',
                                          {'topic': topic})

    async def pause_channel(self, channel: str, topic: str):
        ''' Pause message flow to all channels on an existing channel
        (messages will queue at channel).

        Args:
            topic: the existing topic
            channel: the existing channel to pause
        '''
        return await self.perform_request('POST', 'channel/pause',
                                          {'topic': topic,
                                           'channel': channel})

    async def unpause_channel(self, channel: str, topic: str):
        ''' Resume message flow to consumers of an existing, paused, channel.

        Args:
            topic: the existing topic
            channel: the existing channel to pause
        '''
        return await self.perform_request('POST', 'channel/unpause',
                                          {'topic': topic,
                                           'channel': channel})

    async def debug_pprof(self):
        ''' An index page of available debugging endpoints.
        '''
        return await self.perform_request('GET', 'debug/pprof')

    async def debug_pprof_profile(self):
        ''' Starts a pprof CPU profile for 30s and returns the output
        via the request.

        NOTE: affects runtime performance.
        '''
        return await self.perform_request('GET', 'debug/pprof/profile')

    async def debug_pprof_goroutine(self):
        ''' Returns a stack trace for all running goroutines.
        '''
        return await self.perform_request('GET', 'debug/pprof/goroutine')

    async def debug_pprof_heap(self):
        ''' Returns a heap and memstats profile (top portion can be
        used as a pprof memory profile).
        '''
        return await self.perform_request('GET', 'debug/pprof/heap')

    async def debug_pprof_block(self):
        ''' Returns a goroutine blocking profile.
        '''
        return await self.perform_request('GET', 'debug/pprof/block')

    async def debug_pprof_threadcreate(self):
        ''' Returns goroutine stack traces that led to the creation
        of an OS thread.
        '''
        return await self.perform_request('GET', 'debug/pprof/threadcreate')

    async def get_nsqlookupd_tcp_addresses(self):
        ''' List of nsqlookupd TCP addresses.
        '''
        return await self.perform_request('GET',
                                          'config/nsqlookupd_tcp_addresses')

    async def put_nsqlookupd_tcp_addresses(self, *addresses):
        ''' Update the nsqlookupd TCP addresses.

        Args:
            *addresses: sequence of 'host:port' strings.
        '''
        return await self.perform_request('PUT',
                                          'config/nsqlookupd_tcp_addresses',
                                          body=addresses)

__version__ = '0.3.9'

import asyncio
from .utils import get_host_and_port
from .nsq import Nsq
from .consumer import NsqConsumer


async def create_nsq_producer(host='127.0.0.1', port=4150, loop=None, queue=None,
                              heartbeat_interval=30000, feature_negotiation=True,
                              tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                              sample_rate=0):
    '''
    initial function to get producer
    param: host: host addr with no protocol. 127.0.0.1 
    param: port: host port 
    param: queue: queue where all the msg been put from the nsq 
    param: heartbeat_interval: heartbeat interval with nsq, set -1 to disable nsq heartbeat check
    params: snappy: snappy compress
    params: deflate: deflate compress  can't set True both with snappy
    '''
    # TODO: add parameters type and value validation
    host, tmp_port = get_host_and_port(host)
    if not port:
        port = tmp_port
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


async def create_nsq_consumer(host=None, loop=None, lookupd_poll_interval=5,
                              max_in_flight=42, lookupd_http_addresses=None):
    '''
    initial function to get consumer
    param: host: host addr with no protocol. 127.0.0.1
    param: port: host port
    param: max_in_flight: number of messages get but not finish or req
    param: lookupd_http_addresses: heartbeat interval with nsq, set -1 to disable nsq heartbeat check
    '''
    # TODO: add parameters type and value validation
    if host is None:
        host = ['tcp://127.0.0.1:4150']

    hosts = [get_host_and_port(i) for i in host]
    loop = loop or asyncio.get_event_loop()
    kwargs = {
        'max_in_flight': max_in_flight,
        'loop': loop
    }
    if lookupd_http_addresses:
        kwargs.update({
            'lookupd_http_addresses': lookupd_http_addresses,
            'lookupd_poll_interval': lookupd_poll_interval
        })
    else:
        kwargs['nsqd_tcp_addresses'] = hosts

    conn = NsqConsumer(**kwargs)
    await conn.connect()
    return conn

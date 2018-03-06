import asyncio
from asyncnsq.nsq import create_nsq


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    async def go():
        nsq = await create_nsq(host='127.0.0.1', port=4150,
                               heartbeat_interval=30000,
                               feature_negotiation=True,
                               tls_v1=False,
                               # snappy=True,
                               # deflate=True,
                               deflate_level=0,
                               loop=loop)

        await nsq.pub(b'foo', b'msg foo')
        await nsq.sub(b'foo', b'bar')
        await nsq.rdy(1)
        msg = await nsq.wait_messages()
        assert not msg.processed
        await msg.fin()
        print(msg)
        assert msg.processed
        await nsq.cls()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()

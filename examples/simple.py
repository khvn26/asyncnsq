import asyncio
from asyncnsq.connection import create_connection


def main():

    loop = asyncio.get_event_loop()

    async def go():
        nsq = await create_connection(port=4150, loop=loop)
        resp = await nsq.identify(
            **{
                "client_id": "metrics_increment",
                "hostname": "localhost",
                "heartbeat_interval": 30000,
                "feature_negotiation": True,
                "tls_v1": True,
                # "snappy": True,
                # "sample_rate": 50,
                "deflate": True, "deflate_level": 6,
            })
        # resp = await nsq.execute(b'IDENTIFY',
        # data=json.dumps({"tls_v1": True}))

        print(resp)
        for i in range(0, 100):
            d = b'test_msg: ' + bytes([i])
            print('send ', i, '-----', d)
            await nsq.execute(b'PUB', b'foo', data=d)

        await nsq.execute(b'SUB', b'foo', b'bar')

        for i in range(0, 50):
            await nsq.execute(b'RDY', b'1')

            msg = await nsq._msq_queue.get()

            await nsq.execute(b'FIN', msg.message_id)

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()

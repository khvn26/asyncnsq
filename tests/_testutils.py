import asyncio
import unittest
from functools import wraps


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(fun(test, *args, **kw))
        return ret
    return wrapper


class BaseTest(unittest.TestCase):
    '''Base test case for unittests.
    '''
    def setUp(self):
        self.loop = asyncio.get_event_loop()

        if self.loop.is_closed():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.loop.run_until_complete(self.aioSetUp())

    def tearDown(self):
        self.loop.run_until_complete(self.aioTearDown())

        for task in asyncio.Task.all_tasks():
            task.cancel()

        self.loop.run_until_complete(asyncio.sleep(0.015))
        self.loop.stop()
        self.loop.close()

    async def aioSetUp(self):
        pass

    async def aioTearDown(self):
        pass

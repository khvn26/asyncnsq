import asyncio
import os
import shutil
import subprocess
import unittest
from functools import wraps
from time import sleep


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
    @classmethod
    def setUpClass(cls):
        cls._dir = os.path.abspath('/tmp/__nsqdata')

        try:
            os.mkdir(cls._dir)
        except FileExistsError:
            shutil.rmtree(cls._dir)
            os.mkdir(cls._dir)

        kwargs = {k: subprocess.DEVNULL for k in ('stdout', 'stderr')}
        cls._nsqlookupd = subprocess.Popen(['nsqlookupd'], **kwargs)
        cls._nsqd = subprocess.Popen(['nsqd',
                                      '--data-path={}'.format(cls._dir),
                                      '--lookupd-tcp-address=127.0.0.1:4160'],
                                     **kwargs)
        sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        cls._nsqlookupd.kill()
        cls._nsqd.kill()
        shutil.rmtree(cls._dir)
        sleep(0.5)

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

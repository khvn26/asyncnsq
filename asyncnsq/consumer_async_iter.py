async def messages(self):
    if not self._is_subscribe:
        raise ValueError('You must subscribe to the topic first')

    while self._is_subscribe:
        yield await self._queue.get()

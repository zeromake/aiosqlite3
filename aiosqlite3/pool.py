"""
pool
"""

import asyncio
import collections

def create_pool(
        database=None,
        minsize=1,
        maxsize=10,
        echo=False,
        pool_recycle=-1,
        loop=None,
        **kwargs
    ):
    coro = _create_pool(
        database=database,
        minsize=minsize,
        maxsize=maxsize,
        echo=echo,
        pool_recycle=pool_recycle,
        loop=loop,
        **kwargs
    )


async def _create_pool(
        database=None,
        minsize=1,
        maxsize=10,
        echo=False,
        pool_recycle=-1,
        loop=None,
        **kwargs
    ):
    if loop is None:
        loop = asyncio.get_event_loop()
    pool = Pool(
        database=database,
        minsize=minsize,
        maxsize=maxsize,
        echo=echo,
        pool_recycle=pool_recycle,
        loop=loop,
        **kwargs
    )
    if minsize > 0:
        with (await pool._cond):
            await pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """
    Connection pool
    """

    def __init__(
            self,
            database,
            minsize,
            maxsize,
            echo,
            pool_recycle,
            loop,
            **kwargs
        ):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._database = database
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition(loop=loop)
        self._used = set()
        self._terminated = set()
        self._closing = False
        self._closed = False
        self._echo = echo
        self._recycle = pool_recycle

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

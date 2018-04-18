"""
pool
"""

import asyncio
import collections
from .connection import connect
from .utils import _PoolContextManager, _PoolAcquireContextManager, PY_35
# from .log import logger

__all__ = ['create_pool', 'Pool']


def create_pool(
        database,
        minsize=1,
        maxsize=10,
        echo=False,
        loop=None,
        **kwargs
):
    """
    创建支持上下文管理的pool
    """
    coro = _create_pool(
        database=database,
        minsize=minsize,
        maxsize=maxsize,
        echo=echo,
        loop=loop,
        **kwargs
    )
    return _PoolContextManager(coro)


@asyncio.coroutine
def _create_pool(
        database,
        minsize=1,
        maxsize=10,
        echo=False,
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
        loop=loop,
        **kwargs
    )
    if minsize > 0:
        with (yield from pool._cond):
            yield from pool._fill_free_pool(False)
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
            loop,
            **kwargs
    ):
        if minsize < 0:
            self._closed = True
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            self._closed = True
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

    @property
    def echo(self):
        """
        echo
        """
        return self._echo

    @property
    def minsize(self):
        """
        minsize
        """
        return self._minsize

    @property
    def maxsize(self):
        """
        maxsize
        """
        return self._free.maxlen

    @property
    def size(self):
        """
        size
        """
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        """
        freesize
        """
        return len(self._free)

    @property
    def closed(self):
        """
        closed
        """
        return self._closed

    @asyncio.coroutine
    def clear(self):
        """
        Close all free connections in pool.
        """
        with (yield from self._cond):
            while self._free:
                conn = self._free.popleft()
                yield from conn.close()
            self._cond.notify()

    def close(self):
        """
        Close pool.
        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        # pragma: no cover
        """
        Terminate pool
        """
        self.close()
        for conn in self._used:
            conn.sync_close()
            self._terminated.add(conn)
        self._used.clear()

    @asyncio.coroutine
    def wait_closed(self):
        """
        Wait for closing all pool's connections.
        """
        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(
                ".wait_closed() should be called "
                "after .close()"
            )

        while self._free:
            conn = self._free.popleft()
            if not conn.closed:
                yield from conn.close()
            else:
                # pragma: no cover
                pass
        with (yield from self._cond):
            while self.size > self.freesize:
                yield from self._cond.wait()
        self._used.clear()
        self._closed = True

    def sync_close(self):
        """
        同步关闭
        """
        if self._closed:
            return
        while self._free:
            conn = self._free.popleft()
            if not conn.closed:
                # pragma: no cover
                conn.sync_close()
        for conn in self._used:
            if not conn.closed:
                # pragma: no cover
                conn.sync_close()
            self._terminated.add(conn)
        self._used.clear()
        self._closed = True

    def acquire(self):
        """
        Acquire free connection from the pool.
        """
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    @asyncio.coroutine
    def _acquire(self):
        """
        pool 获得一个 conn
        """
        if self._closing:
            raise RuntimeError(
                "Cannot acquire connection after closing pool"
            )
        with (yield from self._cond):
            while True:
                yield from self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    yield from self._cond.wait()

    @asyncio.coroutine
    def _fill_free_pool(self, override_min):
        """
        iterate over free connections and remove timeouted ones
        """
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = yield from connect(
                    database=self._database,
                    echo=self._echo,
                    loop=self._loop,
                    **self._conn_kwargs
                )
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = yield from connect(
                    database=self._database,
                    echo=self._echo,
                    loop=self._loop,
                    **self._conn_kwargs
                )
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    @asyncio.coroutine
    def _wakeup(self):
        """
        等待
        """
        with (yield from self._cond):
            self._cond.notify()

    @asyncio.coroutine
    def release(self, conn):
        """
        Release free connection back to the connection pool.
        """
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            if self._closing:
                yield from conn.close()
            else:
                self._free.append(conn)
            yield from self._wakeup()

    def __del__(self):
        """
        回收连接
        """
        self.close()
        self.sync_close()
        self._loop = None

    if PY_35:
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
            yield from self.wait_closed()
    else:
        # pragma: no cover
        pass

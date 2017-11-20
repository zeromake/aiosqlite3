"""
生成异步连接对象
"""

import asyncio
import sqlite3
from functools import partial
from .utils import _ContextManager, delegate_to_executor, proxy_property_directly
from .cursor import Cursor

def connect(
        database,
        loop=None,
        executor=None,
        timeout=5,
        echo=False,
        check_same_thread=False,
        **kwargs
    ):
    """
    代理连接
    """
    coro = _connect(
        database,
        loop=loop,
        executor=executor,
        timeout=timeout,
        echo=echo,
        check_same_thread=check_same_thread,
        **kwargs
    )
    return _ContextManager(coro)


async def _connect(
        database,
        loop=None,
        executor=None,
        timeout=5,
        echo=False,
        check_same_thread=False,
        **kwargs
    ):
    if loop is None:
        loop = asyncio.get_event_loop()
    conn = Connection(
        database,
        loop=loop,
        executor=executor,
        timeout=timeout,
        echo=echo,
        check_same_thread=check_same_thread,
        **kwargs
    )
    await conn._connect()
    return conn

@delegate_to_executor(
    '_conn',
    (
        'commit',
        'rollback',
        'create_function',
        'create_aggregate',
        'create_collation',
        'interrupt',
        'set_authorizer',
        'set_progress_handler',
        'set_trace_callback',
        'enable_load_extension',
        'load_extension',
        'iterdump'
    )
)
@proxy_property_directly(
    '_conn',
    (
        'in_transaction',
        'total_changes'
    )
)
class Connection:
    def __init__(
            self,
            database,
            loop=None,
            executor=None,
            timeout=5,
            echo=False,
            check_same_thread=False,
            **kwargs
        ):
        self._database = database
        self._loop = loop or asyncio.get_event_loop()
        self._kwargs = kwargs
        self._executor = executor
        self._echo = echo
        self._timeout = timeout
        self._conn = None
        self._check_same_thread = check_same_thread

    def _execute(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    async def _connect(self):
        func = self._execute(
            sqlite3.connect,
            self._database,
            timeout=self._timeout,
            check_same_thread=self._check_same_thread,
            **self._kwargs
        )
        self._conn = await func
    
    @property
    def loop(self):
        return self._loop

    @property
    def timeout(self):
        return self._timeout

    @property
    def closed(self):
        if self._conn:
            return False
        return True

    @property
    def isolation_level(self) -> str:
        return self._conn.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: str) -> None:
        self._conn.isolation_level = value

    async def _cursor(self, cursor=None):
        """
        获取异步代理cursor对象
        """
        if cursor is None:
            cursor = await self._execute(self._conn.cursor)
        connection = self
        return Cursor(cursor, connection, self._echo)

    def cursor(self):
        """
        转换为上下文模式
        """
        return _ContextManager(self._cursor())

    async def close(self):
        if not self._conn:
            return
        res = await self._execute(self._conn.close)
        self._conn = None
        return res

    async def execute(
            self,
            sql,
            parameters=None,
        ):
        """
        Helper to create a cursor and execute the given query.
        """
        if parameters is None:
            parameters = []
        cursor = await self._execute(self._conn.execute, sql, parameters)
        return _ContextManager(self._cursor(cursor))

    async def executemany(
            self,
            sql,
            parameters,
        ):
        """
        Helper to create a cursor and execute the given multiquery.
        """
        cursor = await self._execute(self._conn.executemany, sql, parameters)
        return _ContextManager(self._cursor(cursor))

    async def executescript(
            self,
            sql_script,
        ):
        """
        Helper to create a cursor and execute a user script.
        """
        cursor = await self._execute(self._conn.executescript, sql_script)
        return _ContextManager(self._cursor(cursor))

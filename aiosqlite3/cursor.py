
"""
代理游标
"""
import asyncio
from .log import logger
from .utils import (
    delegate_to_executor,
    proxy_property_directly,
    PY_35
)

__all__ = ['Cursor']


@delegate_to_executor(
    '_cursor',
    (
        'fetchmany',
        'fetchall'
    )
)
@proxy_property_directly(
    '_cursor',
    (
        'rowcount',
        'lastrowid',
        'description'
    )
)
class Cursor:
    """
    游标
    """
    def __init__(self, cursor, conn, echo=False):
        self._conn = conn
        self._cursor = cursor
        self._loop = conn.loop
        self._echo = echo
        self._executor = None
        self._closed = False

    def _log(self, level, message, *args):
        """
        日志处理
        """
        if self._echo:
            log_fun = getattr(logger, level)
            log_fun(message, *args)

    @asyncio.coroutine
    def _execute(self, func, *args, **kwargs):
        """
        Execute the given function on the shared connection's thread.
        """
        res = yield from self._conn.async_execute(func, *args, **kwargs)
        return res

    @property
    def arraysize(self):
        """
        arraysize
        """
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value):
        """
        set arraysize
        """
        self._cursor.arraysize = value

    @property
    def loop(self):
        """
        loop
        """
        return self._loop

    @property
    def closed(self):
        """
        close
        """
        return self._closed

    @property
    def connection(self):
        """
        返回代理的connection
        """
        return self._conn

    @property
    def native_connection(self):
        """
        返回原生的connection
        """
        return self._cursor.connection

    @property
    def native_cursor(self):
        """
        返回原生的cursor
        """
        return self._cursor

    @asyncio.coroutine
    def fetchone(self):
        """
        获取一条记录
        """
        res = yield from self._execute(self._cursor.fetchone)
        return res

    @asyncio.coroutine
    def execute(self, sql, parameters=None):
        """
        执行sql语句
        """
        self._log(
            'info',
            'cursor.execute->\n  sql: %s\n  args: %s',
            sql,
            str(parameters)
        )
        if parameters is None:
            # pragma: no cover
            parameters = []
        res = yield from self._execute(self._cursor.execute, sql, parameters)
        return res

    @asyncio.coroutine
    def executemany(self, sql, parameters):
        """
        批量执行sql语句
        """
        self._log(
            'info',
            'cursor.executemany->\n  sql: %s\n  args: %s',
            sql,
            str(parameters)
        )
        res = yield from self._execute(
            self._cursor.executemany,
            sql,
            parameters
        )
        return res

    @asyncio.coroutine
    def executescript(self, sql_script):
        """
        执行sql脚本文本
        """
        self._log(
            'info',
            'cursor.executescript->\n  sql_script: %s',
            sql_script
        )
        res = yield from self._execute(self._cursor.executescript, sql_script)
        return res

    @asyncio.coroutine
    def close(self):
        """
        关闭
        """
        if not self._closed:
            yield from self._execute(self._cursor.close)
            self._closed = True

    # def sync_close(self):
    #     """
    #     同步关闭
    #     """
    #     if not self._closed:
    #         self._conn._thread_execute(self._cursor.close)
    #         self._closed = True
    def __enter__(self):
        """
        enter
        """
        return self

    def __exit__(self, exc_type, exc, tbs):
        """
        exit
        """
        if not self._closed:
            self._conn.sync_execute(self._cursor.close)
            self._closed = True

    def __iter__(self):
        """
        iter
        """
        return self

    def __next__(self):
        """
        next
        """
        res = self._conn.sync_execute(self._cursor.fetchone)
        if res is None:
            raise StopIteration
        else:
            return res

    def __del__(self):
        """
        回收引用
        """
        if not self._closed:
            self._conn = None
            self._cursor = None
            self._loop = None
            self._closed = True

    if PY_35:
        @asyncio.coroutine
        def __aiter__(self):
            return self

        @asyncio.coroutine
        def __anext__(self):
            res = yield from self.fetchone()
            if res is None:
                raise StopAsyncIteration
            else:
                return res
    else:
        # pragma: no cover
        pass

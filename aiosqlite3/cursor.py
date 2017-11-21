
"""
代理游标
"""
import asyncio
from .log import logger
from .utils import delegate_to_executor, proxy_property_directly


@delegate_to_executor(
    '_cursor',
    (
        'fetchone',
        'fetchmany',
        'fetchall',
        'close'
    )
)
@proxy_property_directly(
    '_cursor',
    (
        'rowcount',
        'lastrowid',
        'description',
        'connection'
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

    @asyncio.coroutine
    def _execute(self, fn, *args, **kwargs):
        """
        Execute the given function on the shared connection's thread.
        """
        res = yield from self._conn._execute(fn, *args, **kwargs)
        return res

    @property
    def arraysize(self):
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._cursor.arraysize = value

    @asyncio.coroutine
    def execute(self, sql, parameters=[]):
        """
        执行sql语句
        """
        if self._echo:
            logger.info(
                'cursor.execute->\n  sql: %s\n  args: %s',
                sql,
                str(parameters)
            )
        yield from self._execute(self._cursor.execute, sql, parameters)

    @asyncio.coroutine
    def executemany(self, sql, parameters):
        """
        批量执行sql语句
        """
        if self._echo:
            logger.info(
                'cursor.executemany->\n  sql: %s\n  args: %s',
                sql,
                str(parameters)
            )
        yield from self._execute(self._cursor.executemany, sql, parameters)

    @asyncio.coroutine
    def executescript(self, sql_script):
        """
        执行sql脚本文本
        """
        if self._echo:
            logger.info('cursor.executescript->\n  sql_script: %s', sql_script)
        yield from self._execute(self._cursor.executescript, sql_script)

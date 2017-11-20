
"""
代理游标
"""
import asyncio
from .log import logger
from .utils import delegate_to_executor, proxy_property_directly

@delegate_to_executor(
    '_cursor',
    (
        'execute',
        'executemany',
        'executescript',
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

    async def _execute(self, fn, *args, **kwargs):
        """
        Execute the given function on the shared connection's thread.
        """
        res = await self._conn._execute(fn, *args, **kwargs)
        return res

    @property
    def arraysize(self):
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._cursor.arraysize = value

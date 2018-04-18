# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/engine.py
import asyncio
import json
import aiosqlite3
from .connection import SAConnection
from .exc import InvalidRequestError
from ..utils import PY_35, _PoolContextManager, _PoolAcquireContextManager


try:
    from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
except ImportError:  # pragma: no cover
    raise ImportError('aiosqlite.sa requires sqlalchemy')

# see https://www.python.org/dev/peps/pep-0249/#paramstyle
# paramstyle  Meaning
# qmark       Question mark style, e.g. ...WHERE name=?
# numeric     Numeric, positional style, e.g. ...WHERE name=:1
# named       Named style, e.g. ...WHERE name=:name
# format      ANSI C printf format codes, e.g. ...WHERE name=%s
# pyformat 	  Python extended format codes, e.g. ...WHERE name=%(name)s

# _dialect = MySQLDialect_pymysql(paramstyle='named')
# _dialect.default_paramstyle = 'named'

SQLiteCompiler = SQLiteDialect_pysqlite.statement_compiler


class ACompiler_sqlite(SQLiteCompiler):
    """
    让SQLite支持default
    """
    def construct_params(self, params=None, _group_number=None, _check=True):
        compiler_params = super().construct_params(
            params,
            _group_number,
            _check
        )
        for column in self.prefetch:
            compiler_params[column.key] = self._exec_default(
                column.default
            )
        return compiler_params

    def _exec_default(self, default):
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg


def json_deserializer(x):
    # pragma: no cover
    return x


def compiler_dialect(paramstyle='named'):
    """
    构建dialect
    """
    dialect = SQLiteDialect_pysqlite(
        json_serializer=json.dumps,
        json_deserializer=json_deserializer,
        paramstyle=paramstyle
    )
    dialect.default_paramstyle = paramstyle
    dialect.statement_compiler = ACompiler_sqlite
    return dialect


_dialect = compiler_dialect()


def create_engine(
        database,
        minsize=1,
        maxsize=10,
        loop=None,
        dialect=_dialect,
        paramstyle=None,
        **kwargs):
    """
    A coroutine for Engine creation.

    Returns Engine instance with embedded connection pool.

    The pool has *minsize* opened connections to sqlite3.
    """
    coro = _create_engine(
        database=database,
        minsize=minsize,
        maxsize=maxsize,
        loop=loop,
        dialect=dialect,
        paramstyle=paramstyle,
        **kwargs
    )
    return _EngineContextManager(coro)


@asyncio.coroutine
def _create_engine(
        database,
        minsize=1,
        maxsize=10,
        loop=None,
        dialect=_dialect,
        paramstyle=None,
        **kwargs):
    if loop is None:
        # pragma: no cover
        loop = asyncio.get_event_loop()
    pool = yield from aiosqlite3.create_pool(
        database=database,
        minsize=minsize,
        maxsize=maxsize,
        loop=loop,
        **kwargs
    )
    conn = yield from pool.acquire()
    try:
        return Engine(dialect, pool, paramstyle=paramstyle, **kwargs)
    finally:
        # pass
        yield from pool.release(conn)


class Engine:
    """
    Connects a aiosqlite3.Pool and
    sqlalchemy.engine.interfaces.Dialect together to provide a
    source of database connectivity and behavior.

    An Engine object is instantiated publicly using the
    create_engine coroutine.
    """

    def __init__(self, dialect=_dialect, pool=None, paramstyle=None, **kwargs):
        if paramstyle:
            # pragma: no cover
            dialect = compiler_dialect(paramstyle)
        self._dialect = dialect
        self._pool = pool
        self._conn_kw = kwargs

    @property
    def dialect(self):
        """An dialect for engine."""
        return self._dialect

    @property
    def name(self):
        """A name of the dialect."""
        return self._dialect.name

    @property
    def driver(self):
        """A driver of the dialect."""
        return self._dialect.driver

    @property
    def minsize(self):
        return self._pool.minsize

    @property
    def maxsize(self):
        return self._pool.maxsize

    @property
    def size(self):
        return self._pool.size

    @property
    def freesize(self):
        return self._pool.freesize

    def close(self):
        """Close engine.

        Mark all engine connections to be closed on getting back to pool.
        Closed engine doesn't allow to acquire new connections.
        """
        self._pool.close()

    def terminate(self):
        """Terminate engine.

        Terminate engine pool with instantly closing all acquired
        connections also.
        """
        self._pool.terminate()

    @asyncio.coroutine
    def wait_closed(self):
        """
        Wait for closing all engine's connections.
        """
        yield from self._pool.wait_closed()

    def acquire(self):
        """Get a connection from pool."""
        coro = self._acquire()
        return _EngineAcquireContextManager(coro, self)

    @asyncio.coroutine
    def _acquire(self):
        raw = yield from self._pool.acquire()
        conn = SAConnection(raw, self)
        return conn

    @asyncio.coroutine
    def release(self, conn):
        """Revert back connection to pool."""
        if conn.in_transaction:
            raise InvalidRequestError(
                "Cannot release a connection with "
                "not finished transaction"
            )
        raw = conn.connection
        res = yield from self._pool.release(raw)
        return res

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression'
        )

    def __exit__(self, *args):
        # pragma: no cover
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass

    def __iter__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (yield from engine) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from engine.acquire()
        #     try:
        #         <block>
        #     finally:
        #         engine.release(conn)
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

    def __del__(self):
        """
        处理关闭
        """
        self.close()
        self._pool.sync_close()
        self._dialect = None
        self._pool = None
        self._conn_kw = None

    if PY_35:
        # pragma: no cover
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
            yield from self.wait_closed()


_EngineContextManager = _PoolContextManager
_EngineAcquireContextManager = _PoolAcquireContextManager


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from engine) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    __slots__ = ('_engine', '_conn')

    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

    def __enter__(self):
        assert self._conn is not None
        return self._conn

    def __exit__(self, *args):
        try:
            self._engine.release(self._conn)
        finally:
            self._engine = None
            self._conn = None

"""
生成异步连接对象
"""
import concurrent
import asyncio
import sqlite3
from functools import partial
from threading import Event
from queue import Queue

from .sqlite_thread import SqliteThread
from .utils import (
    _ContextManager,
    _LazyloadContextManager,
    delegate_to_executor,
    proxy_property_directly
)
from .cursor import Cursor
from .log import logger

__all__ = ['Connection', 'connect']

_PROXY = (
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
__PROXY = (
    'in_transaction',
    'total_changes'
)


@delegate_to_executor('_conn', _PROXY)
@proxy_property_directly('_conn', __PROXY)
class Connection:
    """
    proxy sqlite3.connection
    """
    def __init__(
            self,
            database,
            loop=None,
            executor=None,
            timeout=5,
            echo=False,
            check_same_thread=False,
            isolation_level='',
            sqlite=sqlite3,
            **kwargs
    ):
        if check_same_thread:
            logger.warning(
                'check_same_thread is True '
                'sqlite on one Thread run'
            )
        self._sqlite = sqlite
        self._database = database
        self._loop = loop or asyncio.get_event_loop()
        self._kwargs = kwargs
        self._executor = executor
        self._echo = echo
        self._timeout = timeout
        self._isolation_level = isolation_level
        self._check_same_thread = check_same_thread
        self._conn = None
        self._closed = False
        if check_same_thread:
            self._thread_lock = asyncio.Lock(loop=loop)
            self.tx_queue = Queue()
            self.rx_queue = Queue()
            self.tx_event = Event()
            self.rx_event = Event()
            self._thread = SqliteThread(
                self.tx_queue,
                self.rx_queue,
                self.tx_event,
                self.rx_event
            )
            self._thread.start()
            self._threading = True
        else:
            self._thread = None
            self._threading = False

    def __enter__(self):
        """
        普通上下文处理
        """
        return self

    def __exit__(self, exc_type, exc, tbs):
        """
        普通上下文处理
        """
        self._loop.call_soon_threadsafe(self.close)

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
        把同步转为async运行
        """
        if self._closed:
            raise TypeError('connection is close')
        func = partial(func, *args, **kwargs)
        if self._check_same_thread:
            future = yield from self._async_thread_execute(func)
        else:
            future = yield from self._loop.run_in_executor(
                self._executor,
                func
            )
        return future

    @asyncio.coroutine
    def async_execute(self, func, *args, **kwargs):
        """
        把同步转为async运行
        """
        return (yield from self._execute(func, *args, **kwargs))

    def sync_execute(self, func, *args, **kwargs):
        """
        同步执行方法
        """
        if self._closed:
            raise TypeError('connection is close')
        func = partial(func, *args, **kwargs)
        if self._check_same_thread:
            return self._thread_execute(func)
        return func()

    @asyncio.coroutine
    def _close_thread(self):
        future = yield from self._async_thread_execute('close')
        self._threading = False
        self._thread = None
        return future

    @asyncio.coroutine
    def _async_thread_execute(self, func):
        """
        通过asyncio的锁每次只执行一个
        """
        with (yield from self._thread_lock):
            func = partial(self._thread_execute, func)
            future = yield from self._loop.run_in_executor(
                self._executor,
                func
            )
        return future

    def _thread_execute(self, func):
        """
        通知线程执行任务
        """
        self.tx_queue.put(func)
        self.tx_event.set()
        self.rx_event.wait()
        self.rx_event.clear()
        result = self.rx_queue.get_nowait()
        if isinstance(result, Exception):
            # pragma: no cover
            raise result
        return result

    @asyncio.coroutine
    def _connect(self):
        """
        async连接，必须使用多线程模式
        """
        func = yield from self._execute(
            self._sqlite.connect,
            self._database,
            timeout=self._timeout,
            isolation_level=self._isolation_level,
            check_same_thread=self._check_same_thread,
            **self._kwargs
        )
        self._conn = func
        self._log(
            'debug',
            'connect-> "%s" ok',
            self._database
        )

    @asyncio.coroutine
    def connect(self):
        """
        connect
        """
        return (yield from self._connect())

    @property
    def echo(self):
        """
        日志输出开关
        """
        return self._echo

    @property
    def loop(self):
        """
        连接使用的loop
        """
        return self._loop

    @property
    def timeout(self):
        """
        超时时间
        """
        return self._timeout

    @property
    def closed(self):
        """
        是否已关闭连接
        """
        return self._closed

    @property
    def autocommit(self):
        """
        是否为自动commit
        """
        return self._conn.isolation_level is None

    @property
    def isolation_level(self):
        """
        智能,自动commit
        """
        return self._conn.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: str) -> None:
        """
        事物等级
        """
        if self._check_same_thread:
            func = partial(self._sync_setter, 'isolation_level', value)
            self._thread_execute(func)
        else:
            self._conn.isolation_level = value

    @property
    def row_factory(self):
        """
        row_factory
        """
        return self._conn.row_factory

    def _sync_setter(self, field, value):
        """
        同步设置属性
        """
        setattr(self._conn, field, value)

    @row_factory.setter
    def row_factory(self, value):
        """
        set row_factory
        """
        if self._check_same_thread:
            func = partial(self._sync_setter, 'row_factory', value)
            self._thread_execute(func)
        else:
            self._conn.row_factory = value

    @property
    def text_factory(self):
        """
        text_factory
        """
        return self._conn.text_factory

    @text_factory.setter
    def text_factory(self, value):
        """
        set text_factory
        """
        if self._check_same_thread:
            func = partial(self._sync_setter, 'text_factory', value)
            self._thread_execute(func)
        else:
            self._conn.text_factory = value

    def _create_cursor(self, cursor):
        """
        创建代理cursor
        """
        return Cursor(cursor, self, self._echo)

    def _create_context_cursor(self, coro):
        """
        创建支持await上下文cursor
        """
        return _LazyloadContextManager(coro, self._create_cursor)

    def cursor(self):
        """
        转换为上下文模式
        """
        coro = self._execute(self._conn.cursor)
        return self._create_context_cursor(coro)

    @asyncio.coroutine
    def close(self):
        """
        关闭
        """
        if self._closed or self._conn is None:
            return
        yield from self._execute(self._conn.close)
        if self._check_same_thread:
            yield from self._close_thread()
            self._thread = None
        self._closed = True
        self._log(
            'debug',
            'close-> "%s" ok',
            self._database
        )

    def execute(
            self,
            sql,
            parameters=None,
    ):
        """
        Helper to create a cursor and execute the given query.
        """
        self._log(
            'info',
            'connection.execute->\n  sql: %s\n  args: %s',
            sql,
            str(parameters)
        )
        if parameters is None:
            parameters = []
        coro = self._execute(self._conn.execute, sql, parameters)
        return self._create_context_cursor(coro)

    @asyncio.coroutine
    def executemany(
            self,
            sql,
            parameters,
    ):
        """
        Helper to create a cursor and execute the given multiquery.
        """
        self._log(
            'info',
            'connection.executemany->\n  sql: %s\n  args: %s',
            sql,
            str(parameters)
        )
        coro = self._execute(
            self._conn.executemany,
            sql,
            parameters
        )
        return self._create_context_cursor(coro)

    def executescript(
            self,
            sql_script,
    ):
        """
        Helper to create a cursor and execute a user script.
        """
        self._log(
            'info',
            'connection.executescript->\n  sql_script: %s',
            sql_script
        )
        coro = self._execute(
            self._conn.executescript,
            sql_script
        )
        return self._create_context_cursor(coro)

    def sync_close(self):
        """
        同步关闭连接
        """
        self.__del__()

    def __del__(self):
        """
        关闭连接清理线程
        """
        if not self._closed:
            if self._check_same_thread:
                if self._thread:
                    self._thread_execute(self._conn.close)
                    self._thread_execute('close')
                    self._thread = None
                    self._thread_lock = None
                    self.tx_queue = None
                    self.rx_queue = None
                    self.tx_event = None
                    self.rx_event = None
                else:
                    # pragma: no cover
                    pass
            else:
                self._conn.close()
            self._conn = None
            self._sqlite = None
            database = self._database
            self._database = None
            self._loop = None
            self._kwargs = None
            self._executor = None
            self._closed = True
            self._log(
                'debug',
                '__del__ close-> "%s" ok',
                database
            )


def connect(
        database: str,
        loop: asyncio.BaseEventLoop = None,
        executor: concurrent.futures.Executor = None,
        timeout: int = 5,
        echo: bool = False,
        isolation_level: str = '',
        check_same_thread: bool = False,
        **kwargs: dict
):
    """
    把async方法执行后的对象创建为async上下文模式
    """
    coro = _connect(
        database,
        loop=loop,
        executor=executor,
        timeout=timeout,
        echo=echo,
        isolation_level=isolation_level,
        check_same_thread=check_same_thread,
        **kwargs
    )
    return _ContextManager(coro)


@asyncio.coroutine
def _connect(
        database: str,
        loop: asyncio.BaseEventLoop = None,
        executor: concurrent.futures.Executor = None,
        timeout: int = 5,
        echo: bool = False,
        isolation_level: str = '',
        check_same_thread: bool = False,
        **kwargs: dict
):
    """
    async 方法代理
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    conn = Connection(
        database,
        loop=loop,
        executor=executor,
        timeout=timeout,
        echo=echo,
        isolation_level=isolation_level,
        check_same_thread=check_same_thread,
        **kwargs
    )
    yield from conn.connect()
    return conn

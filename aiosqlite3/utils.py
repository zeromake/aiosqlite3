"""
代理方法工具
"""
import asyncio
import sys

PY_35 = sys.version_info >= (3, 5)

if PY_35:
    from collections.abc import Coroutine
    base = Coroutine
else:
    base = object


class _ContextManager(base):
    __slots__ = ('_coro', '_obj')

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    @asyncio.coroutine
    def __iter__(self):
        resp = yield from self._coro
        return resp

    if PY_35:
        def __await__(self):
            resp = yield from self._coro
            return resp

        @asyncio.coroutine
        def __aenter__(self):
            self._obj = yield from self._coro
            return self._obj

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            yield from self._obj.close()
            self._obj = None


class _PoolContextManager(_ContextManager):
    if PY_35:
        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            """
            async with or with (yield from xx) exit
            """
            self._obj.close()
            yield from self._obj.wait_closed()
            self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ('_coro', '_conn', '_pool')

    def __init__(self, coro, pool):
        self._coro = coro
        self._conn = None
        self._pool = pool

    if PY_35:
        @asyncio.coroutine
        def __aenter__(self):
            self._conn = yield from self._coro
            return self._conn

        @asyncio.coroutine
        def __aexit__(self):
            try:
                yield from self._pool.release(self._conn)
            finally:
                self._pool = None
                self._conn = None


if not PY_35:
    try:
        from asyncio import coroutines
        coroutines._COROUTINE_TYPES += (_ContextManager,)
    except Exception as error:
        pass


def delegate_to_executor(bind_attr, attrs):
    """
    为类添加异步代理方法
    args:
        attrs: list -> 需要代理的原方法
    """
    def cls_builder(cls):
        """
        添加到类
        """
        for attr_name in attrs:
            setattr(
                cls,
                attr_name,
                _make_delegate_method(bind_attr, attr_name)
            )
        return cls
    return cls_builder


def proxy_method_directly(bind_attr, attrs):
    """
    为类添加代理方法
    """
    def cls_builder(cls):
        """
        添加到类
        """
        for attr_name in attrs:
            setattr(cls, attr_name, _make_proxy_method(bind_attr, attr_name))
        return cls

    return cls_builder


def proxy_property_directly(bind_attr, attrs):
    """
    为类添加代理属性
    """
    def cls_builder(cls):
        """
        添加到类
        """
        for attr_name in attrs:
            setattr(cls, attr_name, _make_proxy_property(bind_attr, attr_name))
        return cls
    return cls_builder


def _make_delegate_method(bind_attr, attr_name):
    @asyncio.coroutine
    def method(self, *args, **kwargs):
        bind = getattr(self, bind_attr)
        func = getattr(bind, attr_name)
        res = yield from self._execute(func, *args, **kwargs)
        return res
    return method


def _make_proxy_method(bind_attr, attr_name):
    def method(self, *args, **kwargs):
        bind = getattr(self, bind_attr)
        return getattr(bind, attr_name)(*args, **kwargs)
    return method


def _make_proxy_property(bind_attr, attr_name):
    def proxy_property(self):
        bind = getattr(self, bind_attr)
        return getattr(bind, attr_name)
    return property(proxy_property)

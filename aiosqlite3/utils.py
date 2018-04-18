"""
代理方法工具
"""
import asyncio
import sys

PY_35 = sys.version_info >= (3, 5)

if PY_35:
    from collections.abc import Coroutine
    BASE = Coroutine
else:
    # pragma: no cover
    BASE = object


def create_future(loop):
    # pragma: no cover
    """Compatibility wrapper for the loop.create_future() call introduced in
    3.5.2."""
    if hasattr(loop, 'create_future'):
        return loop.create_future()
    return asyncio.Future(loop=loop)


def create_task(coro, loop):
    # pragma: no cover
    """Compatibility wrapper for the loop.create_task() call introduced in
    3.4.2."""
    if hasattr(loop, 'create_task'):
        return loop.create_task(coro)
    return asyncio.Task(coro, loop=loop)


class _ContextManager(BASE):
    __slots__ = ('_coro', '_obj')

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        # pragma: no cover
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        return self._coro.throw(typ, val, tb)

    def close(self):
        # pragma: no cover
        return self._coro.close()

    @property
    def gi_frame(self):
        # pragma: no cover
        """
        gi_frame
        """
        return self._coro.gi_frame

    @property
    def gi_running(self):
        # pragma: no cover
        """
        gi_runing
        """
        return self._coro.gi_running

    @property
    def gi_code(self):
        # pragma: no cover
        """
        gi_code
        """
        return self._coro.gi_code

    def __next__(self):
        # pragma: no cover
        return self.send(None)

    # @asyncio.coroutine
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
        def __aexit__(self, exc_type, exc, tbs):
            yield from self._obj.close()
            self._obj = None
    else:
        # pragma: no cover
        pass


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
    else:
        # pragma: no cover
        pass


class _LazyloadContextManager(_ContextManager):
    """
    延迟上下文
    """
    __slots__ = ('_coro', '_obj', '_lazyload')

    def __init__(self, coro, lazyload):
        super(_LazyloadContextManager, self).__init__(coro)
        if not lazyload:
            # pragma: no cover
            raise TypeError('lazyload is function')
        # self._coro = coro
        # self._obj = None
        self._lazyload = lazyload

    @asyncio.coroutine
    def __iter__(self):
        resp = yield from self._coro
        resp = self._lazyload(resp)
        return resp

    if PY_35:
        def __await__(self):
            resp = yield from self._coro
            resp = self._lazyload(resp)
            return resp

        @asyncio.coroutine
        def __aenter__(self):
            resp = yield from self._coro
            self._obj = self._lazyload(resp)
            return self._obj

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            yield from self._obj.close()
            self._obj = None

    else:
        # pragma: no cover
        pass


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ('_coro', '_conn', '_pool')

    def __init__(self, coro, pool):
        super(_PoolAcquireContextManager, self).__init__(coro)
        # self._coro = coro
        self._conn = None
        self._pool = pool

    if PY_35:
        @asyncio.coroutine
        def __aenter__(self):
            self._conn = yield from self._coro
            return self._conn

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            try:
                yield from self._pool.release(self._conn)
            finally:
                self._pool = None
                self._conn = None
    else:
        # pragma: no cover
        pass


if not PY_35:
    # pragma: no cover
    try:
        from asyncio import coroutines
        coroutines._COROUTINE_TYPES += (_ContextManager,)
    except TypeError:
        pass
else:
    pass


class _SAConnectionContextManager(_ContextManager):
    if PY_35:
        # pragma: no cover
        @asyncio.coroutine
        def __aiter__(self):
            result = yield from self._coro
            return result


class _TransactionContextManager(_ContextManager):

    if PY_35:
        # pragma: no cover

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            if exc_type:
                yield from self._obj.rollback()
            else:
                if self._obj.is_active:
                    yield from self._obj.commit()
            yield from self._obj.close()
            self._obj = None


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


# def proxy_method_directly(bind_attr, attrs):
#     """
#     为类添加代理方法
#     """
#     def cls_builder(cls):
#         """
#         添加到类
#         """
#         for attr_name in attrs:
#             setattr(cls, attr_name, _make_proxy_method(bind_attr, attr_name))
#         return cls

#     return cls_builder


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
        """
        method
        """
        bind = getattr(self, bind_attr)
        func = getattr(bind, attr_name)
        res = yield from self._execute(func, *args, **kwargs)
        return res
    return method


# def _make_proxy_method(bind_attr, attr_name):
#     def method(self, *args, **kwargs):
#         bind = getattr(self, bind_attr)
#         return getattr(bind, attr_name)(*args, **kwargs)
#     return method


def _make_proxy_property(bind_attr, attr_name):
    def proxy_property(self):
        """
        proxy
        """
        bind = getattr(self, bind_attr)
        return getattr(bind, attr_name)
    return property(proxy_property)


import asyncio
import functools
import sys
from collections.abc import Coroutine

PY_35 = sys.version_info >= (3, 5)


class _ContextManager(Coroutine):
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

    def __await__(self):
        resp = self._coro.__await__()
        return resp

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        await self._obj.close()
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
            setattr(cls, attr_name, _make_delegate_method(bind_attr, attr_name))
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
    async def method(self, *args, **kwargs):
        bind = getattr(self, bind_attr)
        func = getattr(bind, attr_name)
        return await self._execute(func, *args, **kwargs)
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


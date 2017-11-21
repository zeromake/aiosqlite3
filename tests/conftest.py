import asyncio
import pytest
import gc
import aiosqlite3
import logging

logging.basicConfig(
    level=logging.DEBUG
)


@pytest.fixture(scope='session')
def loop():
    """
    生成loop
    """
    loop_obj = asyncio.new_event_loop()
    yield loop_obj
    gc.collect()
    loop_obj.close()

@pytest.fixture
async def conn(loop):
    """
    生成一个连接
    """
    pass

@pytest.fixture
async def connection_maker(loop):
    """
    代理生成连接，并回收
    """
    cleanup = []

    async def make():
        conn_obj = await aiosqlite3.connect(':memory:', echo=True)
        cleanup.append(conn_obj)
        return conn_obj
    yield make
    for conn_obj in cleanup:
        await conn_obj.close()

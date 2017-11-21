import sys, os
sys.path.append((os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))))

import asyncio
import gc
import logging

import aiosqlite3
import pytest

logging.basicConfig(
    level=logging.DEBUG
)

@pytest.fixture(scope='session')
def event_loop():
    loop_obj = asyncio.new_event_loop()
    yield loop_obj
    gc.collect()
    loop_obj.close()

@pytest.fixture(scope='session')
def loop(event_loop):
    """
    生成loop
    """
    return event_loop

@pytest.fixture
async def conn(loop, connection_maker):
    """
    生成一个连接
    """
    connection = await connection_maker()
    yield connection

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

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import asyncio
import gc
import logging

import aiosqlite3
import pytest
from concurrent.futures import ThreadPoolExecutor

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
def conn(loop, db):
    """
    生成一个连接
    """
    coro = aiosqlite3.connect(database=db, echo=True)
    connection = loop.run_until_complete(coro)
    yield connection
    loop.run_until_complete(connection.close())

@pytest.fixture
def make_conn(loop, db):
    """
    创建一个连接方法
    """
    conns = []
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from aiosqlite3.connect(database=db, loop=loop, echo=True, **kwargs)
        conns.append(conn)
        return conn
    yield go
    for conn in conns:
        if not conn.closed:
            loop.run_until_complete(conn.close())

@pytest.fixture
def db():
    return ':memory:'

@pytest.fixture
def executor():
    executor = ThreadPoolExecutor(max_workers=1)
    yield executor
    executor.shutdown()


@pytest.fixture
async def pool(loop, db):
    pool = await aiosqlite3.create_pool(loop=loop, database=db)
    yield pool

    pool.close()
    await pool.wait_closed()


@pytest.fixture
async def pool_maker(loop):
    pool_list = []

    async def make(loop, **kw):
        pool = await aiosqlite3.create_pool(loop=loop, **kw)
        pool_list.append(pool)
        return pool

    yield make

    for pool in pool_list:
        pool.close()
        await pool.wait_closed()


@pytest.fixture
async def cursor(conn):
    """
    获取默认cursor
    """
    cursor_obj = await conn.cursor()
    yield cursor_obj
    await cursor_obj.close()

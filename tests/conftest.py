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
def db():
    return ':memory:'

@pytest.fixture
def executor():
    executor = ThreadPoolExecutor(max_workers=1)
    yield executor
    executor.shutdown()

@pytest.fixture
@asyncio.coroutine
def pool(loop, db):
    pool = yield from aiosqlite3.create_pool(loop=loop, database=db)
    yield pool

    pool.close()
    yield from pool.wait_closed()

@pytest.fixture
@asyncio.coroutine
def pool_maker(loop):
    pool_list = []

    def make(loop, **kw):
        pool = yield from aiosqlite3.create_pool(loop=loop, **kw)
        pool_list.append(pool)
        return pool

    yield make

    for pool in pool_list:
        pool.close()
        yield from pool.wait_closed()

import asyncio

import pytest

import aiosqlite3
from aiosqlite3 import Pool, Connection
from tests.utils import PY_35


@pytest.mark.asyncio
@asyncio.coroutine
def test_create_pool(loop, pool_maker, db):
    """
    测试创建一个pool
    """
    pool = yield from pool_maker(loop, database=db)
    assert isinstance(pool, Pool)
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1
    assert not pool.echo

@pytest.mark.asyncio
@asyncio.coroutine
def test_acquire(pool):
    """
    测试pool的acquire
    """
    conn = yield from pool.acquire()
    try:
        assert isinstance(conn, Connection)
        assert not conn.closed
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1')
        val = yield from cur.fetchone()
        assert (1,) == tuple(val)
    finally:
        yield from pool.release(conn)

@pytest.mark.asyncio
@asyncio.coroutine
def test_release(pool):
    conn = yield from pool.acquire()
    try:
        assert pool.freesize == 0
        assert {conn} == pool._used
    finally:
        yield from pool.release(conn)
    assert pool.freesize == 1
    assert not pool._used

@pytest.mark.asyncio
@asyncio.coroutine
def test_release_closed(pool):
    conn = yield from pool.acquire()
    assert pool.freesize == 0
    yield from conn.close()
    yield from pool.release(conn)
    assert pool.freesize == 0
    assert not pool._used
    assert pool.size == 0

    conn2 = yield from pool.acquire()
    assert pool.freesize == 0
    assert pool.size == 1
    yield from pool.release(conn2)

@pytest.mark.asyncio
@asyncio.coroutine
def test_context_manager(pool):
    conn = yield from pool.acquire()
    try:
        assert isinstance(conn, Connection)
        assert pool.freesize == 0
        assert {conn} == pool._used
    finally:
        yield from pool.release(conn)
    assert pool.freesize == 1

@pytest.mark.asyncio
@asyncio.coroutine
def test_clear(pool):
    yield from pool.clear()
    assert pool.freesize == 0


@pytest.mark.asyncio
@asyncio.coroutine
def test_initial_empty(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=0)

    assert pool.maxsize == 10
    assert pool.minsize == 0
    assert pool.size == 0
    assert pool.freesize == 0

    conn = yield from pool.acquire()
    try:
        assert pool.size == 1
        assert pool.freesize == 0
    finally:
        yield from pool.release(conn)
    assert pool.size == 1
    assert pool.freesize == 1

    conn1 = yield from pool.acquire()
    assert pool.size == 1
    assert pool.freesize == 0

    conn2 = yield from pool.acquire()
    assert pool.size == 2
    assert pool.freesize == 0

    yield from pool.release(conn1)
    assert pool.size == 2
    assert pool.freesize == 1

    yield from pool.release(conn2)
    assert pool.size == 2
    assert pool.freesize == 2


@pytest.mark.asyncio
@asyncio.coroutine
def test_parallel_tasks(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=0, maxsize=2)

    assert pool.maxsize == 2
    assert pool.minsize == 0
    assert pool.size == 0
    assert pool.freesize == 0

    fut1 = pool.acquire()
    fut2 = pool.acquire()

    conn1, conn2 = yield from asyncio.gather(fut1, fut2, loop=loop)
    assert pool.size == 2
    assert pool.freesize == 0
    assert {conn1, conn2} == pool._used

    yield from pool.release(conn1)
    assert pool.size == 2
    assert pool.freesize == 1
    assert {conn2} == pool._used

    yield from pool.release(conn2)
    assert pool.size == 2
    assert pool.freesize == 2
    assert not conn1.closed
    assert not conn2.closed

    conn3 = yield from pool.acquire()
    assert conn3 is conn1
    yield from pool.release(conn3)

@pytest.mark.asyncio
@asyncio.coroutine
def test_parallel_tasks_more(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=0, maxsize=3)

    fut1 = pool.acquire()
    fut2 = pool.acquire()
    fut3 = pool.acquire()

    conn1, conn2, conn3 = yield from asyncio.gather(fut1, fut2, fut3,
                                               loop=loop)
    assert pool.size == 3
    assert pool.freesize == 0
    assert {conn1, conn2, conn3} == pool._used

    yield from pool.release(conn1)
    assert pool.size == 3
    assert pool.freesize == 1
    assert {conn2, conn3} == pool._used

    yield from pool.release(conn2)
    assert pool.size == 3
    assert pool.freesize == 2
    assert {conn3} == pool._used
    assert not conn1.closed
    assert not conn2.closed

    yield from pool.release(conn3)
    assert pool.size == 3
    assert pool.freesize == 3
    assert not pool._used
    assert not conn1.closed
    assert not conn2.closed
    assert not conn3.closed

    conn4 = yield from pool.acquire()
    assert conn4 is conn1
    yield from pool.release(conn4)

@pytest.mark.asyncio
@asyncio.coroutine
def test_default_loop(loop, db):
    pool = yield from aiosqlite3.create_pool(database=db)
    assert pool._loop is loop
    pool.close()
    yield from pool.wait_closed()

@pytest.mark.asyncio
@asyncio.coroutine
def test__fill_free(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=1)

    first_conn = yield from pool.acquire()
    try:
        assert pool.freesize == 0
        assert pool.size == 1

        conn = yield from asyncio.wait_for(pool.acquire(), timeout=0.5,
                                      loop=loop)
        assert pool.freesize == 0
        assert pool.size == 2
        yield from pool.release(conn)
        assert pool.freesize == 1
        assert pool.size == 2
    finally:
        yield from pool.release(first_conn)
    assert pool.freesize == 2
    assert pool.size == 2


@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_from_acquire(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=0)

    assert pool.freesize == 0
    assert pool.size == 0
    conn = yield from pool.acquire()
    try:
        assert pool.size == 1
        assert pool.freesize == 0
    finally:
        yield from pool.release(conn)
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.asyncio
@asyncio.coroutine
def test_concurrency(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=2, maxsize=4)

    c1 = yield from pool.acquire()
    c2 = yield from pool.acquire()
    assert pool.freesize == 0
    assert pool.size == 2
    yield from pool.release(c1)
    yield from pool.release(c2)


@pytest.mark.asyncio
@asyncio.coroutine
def test_invalid_minsize_and_maxsize(loop, db):
    with pytest.raises(ValueError):
        yield from aiosqlite3.create_pool(
            database=db,
            loop=loop,
            minsize=-1
        )

    with pytest.raises(ValueError):
        yield from aiosqlite3.create_pool(
            database=db,
            loop=loop,
            minsize=5,
            maxsize=2
        )



@pytest.mark.asyncio
@asyncio.coroutine
def test_true_parallel_tasks(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, minsize=0, maxsize=1)

    assert pool.maxsize == 1
    assert pool.minsize == 0
    assert pool.size == 0
    assert pool.freesize == 0

    maxsize = 0
    minfreesize = 100


    @asyncio.coroutine
    def inner():
        nonlocal maxsize, minfreesize
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        conn = yield from pool.acquire()
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        yield from asyncio.sleep(0.01, loop=loop)
        yield from pool.release(conn)
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)

    yield from asyncio.gather(inner(), inner(), loop=loop)

    assert maxsize == 1
    assert minfreesize == 0


@pytest.mark.asyncio
@asyncio.coroutine
def test_cannot_acquire_after_closing(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db)

    pool.close()

    with pytest.raises(RuntimeError):
        yield from pool.acquire()


@pytest.mark.asyncio
@asyncio.coroutine
def test_wait_closed(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db)

    c1 = yield from pool.acquire()
    c2 = yield from pool.acquire()
    assert pool.size == 2
    assert pool.freesize == 0

    ops = []

    @asyncio.coroutine
    def do_release(conn):
        yield from asyncio.sleep(0, loop=loop)
        yield from pool.release(conn)
        ops.append('release')

    @asyncio.coroutine
    def wait_closed():
        yield from pool.wait_closed()
        ops.append('wait_closed')

    pool.close()
    yield from asyncio.gather(
        do_release(c1),
        do_release(c2),
        wait_closed(),
        loop=loop
    )
    assert len(ops) == 3
    assert 'wait_closed' in ops
    assert 'release' in ops
    assert pool.freesize == 0


@pytest.mark.asyncio
@asyncio.coroutine
def test_echo(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db, echo=True)

    assert pool.echo
    conn = yield from pool.acquire()
    assert conn.echo
    yield from pool.release(conn)


@pytest.mark.asyncio
@asyncio.coroutine
def test_release_closed_connection(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db)

    conn = yield from pool.acquire()
    yield from conn.close()

    yield from pool.release(conn)
    pool.close()


@pytest.mark.asyncio
@asyncio.coroutine
def test_wait_closing_on_not_closed(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db)

    with pytest.raises(RuntimeError):
        yield from pool.wait_closed()
    pool.close()


@pytest.mark.asyncio
@asyncio.coroutine
def test_close_with_acquired_connections(loop, pool_maker, db):
    pool = yield from pool_maker(loop, database=db)

    conn = yield from pool.acquire()
    pool.close()

    with pytest.raises(asyncio.TimeoutError):
        yield from asyncio.wait_for(pool.wait_closed(), 0.1, loop=loop)
    yield from conn.close()
    yield from pool.release(conn)


@pytest.mark.asyncio
@asyncio.coroutine
def test_pool_with_executor(loop, pool_maker, db, executor):
    pool = yield from pool_maker(
        loop,
        executor=executor,
        database=db,
        minsize=2,
        maxsize=2
    )

    conn = yield from pool.acquire()
    try:
        assert isinstance(conn, Connection)
        assert not conn.closed
        assert conn._executor is executor
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1')
        val = yield from cur.fetchone()
        assert (1,) == tuple(val)
    finally:
        yield from pool.release(conn)
    # we close pool here instead in finalizer because of pool should be
    # closed before executor
    pool.close()
    yield from pool.wait_closed()

@pytest.mark.asyncio
async def test_pool_context_manager(loop, pool):
    assert not pool.closed
    async with pool:
        assert not pool.closed
    assert pool.closed


@pytest.mark.asyncio
async def test_pool_context_manager2(loop, pool):
    async with pool.acquire() as conn:
        assert not conn.closed
        cur = await conn.cursor()
        await cur.execute('SELECT 1')
        val = await cur.fetchone()
        assert (1,) == tuple(val)


@pytest.mark.asyncio
async def test_all_context_managers(db, loop, executor):
    kw = dict(database=db, loop=loop, executor=executor)
    async with aiosqlite3.create_pool(**kw) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                assert not pool.closed
                assert not conn.closed
                assert not cur.closed

                await cur.execute('SELECT 1')
                val = await cur.fetchone()
                assert (1,) == tuple(val)

    assert pool.closed
    assert conn.closed
    assert cur.closed

@pytest.mark.asyncio
@asyncio.coroutine
def test_pool_del(db, loop):
    """
    测试内存回收
    """
    @asyncio.coroutine
    def make():
        pool = yield from aiosqlite3.create_pool(database=db, loop=loop)
        conn = yield from pool.acquire()
    yield from make()

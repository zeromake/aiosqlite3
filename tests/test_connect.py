"""
测试连接
"""
import asyncio
import pytest

import aiosqlite3

def test_connect(loop, conn):
    """
    测试连接对象属性
    """
    assert conn.loop is loop
    assert conn.timeout == 5
    assert not conn.closed
    assert not conn.autocommit
    assert conn.isolation_level is ''

@pytest.mark.asyncio
@asyncio.coroutine
def test_basic_corsor(conn):
    """
    测试连接对象的游标获得
    """
    cursor = yield from conn.cursor()
    sql = 'SELECT 10;'
    yield from cursor.execute(sql)
    (resp,) = yield from cursor.fetchone()
    yield from cursor.close()
    assert resp == 10

@pytest.mark.asyncio
@asyncio.coroutine
def test_default_loop(loop, db):
    """
    测试连接对象的loop设置
    """
    asyncio.set_event_loop(loop)
    conn = yield from aiosqlite3.connect(db)
    assert conn.loop is loop
    yield from conn.close()


@pytest.mark.asyncio
@asyncio.coroutine
def test_close_twice(conn):
    """
    测试连接关闭
    """
    assert conn.closed, False
    yield from conn.close()
    yield from conn.close()
    assert conn.closed, True

@pytest.mark.asyncio
@asyncio.coroutine
def test_autocommit(loop, db):
    """
    测试自定义的autocommit属性
    """
    conn = yield from aiosqlite3.connect(db, isolation_level=None, loop=loop)
    assert conn.autocommit, True


@pytest.mark.asyncio
@asyncio.coroutine
def test_rollback(conn):
    """
    测试智能commit的事务
    """
    assert not conn.autocommit

    cur = yield from conn.cursor()
    yield from cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")

    yield from conn.commit()

    yield from cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
    yield from cur.execute("SELECT v FROM t1")
    (value,) = yield from cur.fetchone()
    assert value == '123.45'

    yield from conn.rollback()
    yield from cur.execute("SELECT v FROM t1;")
    value = yield from cur.fetchone()
    assert value is None
    yield from cur.execute("DROP TABLE t1;")
    yield from conn.commit()

    yield from conn.close()

@pytest.mark.asyncio
@asyncio.coroutine
def test_custom_executor(loop, db, executor):
    conn = yield from aiosqlite3.connect(db, executor=executor, loop=loop)
    assert conn._executor is executor
    cur = yield from conn.execute('SELECT 10;')
    (resp,) = yield from cur.fetchone()
    yield from conn.close()
    assert resp == 10
    assert conn.closed

@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_context_manager(loop, db):
    """
    上下文支持
    """
    with (yield from aiosqlite3.connect(db, loop=loop)) as conn:
        assert not conn.closed
    assert conn.closed

"""
测试连接
"""
import asyncio
import pytest

import aiosqlite3
from tests.utils import PY_35

def test_connect(loop, conn):
    """
    测试连接对象属性
    """
    assert conn.loop is loop
    assert conn.timeout == 5
    assert not conn.closed
    assert not conn.autocommit
    assert conn.isolation_level is ''
    assert conn.row_factory is None
    assert conn.text_factory is str

def test_connect_setter(conn):
    """
    测试connect的setter
    """
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    def unicode_str(x):
        return unicode(x, "utf-8", "ignore")

    assert not conn.autocommit
    conn.isolation_level = None
    assert conn.autocommit
    assert conn.row_factory is None
    assert conn.text_factory is str
    conn.row_factory = dict_factory
    assert conn.row_factory is dict_factory

    conn.text_factory = unicode_str
    assert conn.text_factory is unicode_str

@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_sync_setter(db):
    """
    测试connect的setter
    """
    conn = yield from aiosqlite3.connect(database=db, echo=True, check_same_thread=True)
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    def unicode_str(x):
        return unicode(x, "utf-8", "ignore")

    assert not conn.autocommit
    conn.isolation_level = None
    assert conn.autocommit
    assert conn.row_factory is None
    assert conn.text_factory is str
    conn.row_factory = dict_factory
    assert conn.row_factory is dict_factory

    conn.text_factory = unicode_str
    assert conn.text_factory is unicode_str
    yield from conn.close()

@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_del(db, loop):
    @asyncio.coroutine
    def test():
        conn = yield from aiosqlite3.connect(
            ':memory:',
            loop=loop,
            check_same_thread=True
        )
    yield from test()

@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_execute(conn):
    """
    测试execute
    """
    assert conn.echo
    sql = 'SELECT 10'
    cursor = yield from conn.execute(sql, [])
    (resp,) = yield from cursor.fetchone()
    yield from cursor.close()
    assert resp == 10

@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_executemany(conn):
    """
    测试批量执行
    """
    def char_generator(start=1):
        for c in range(start, start+2):
            yield (c, str(c))
    create_table_sql = '''CREATE TABLE `student` (
                            `id` int(11) NOT NULL,
                            `name` varchar(20) NOT NULL,
                            PRIMARY KEY (`id`)
                        )'''
    yield from conn.execute(create_table_sql)
    yield from conn.commit()
    yield from conn.executemany('insert into student(id, name) values(?,?)', char_generator())
    yield from conn.commit()
    cursor = yield from conn.execute('SELECT * FROM student')
    assert cursor.rowcount == -1
    resp = yield from cursor.fetchone()
    index = 1
    while resp:
        assert resp == (index, str(index))
        resp = yield from cursor.fetchone()
        index += 1
    assert index == 3

@pytest.mark.asyncio
async def test_connect_executescript(db):
    """
    测试executescript
    """
    conn = await aiosqlite3.connect(database=db)
    await conn.execute('''CREATE TABLE `student` (
                            `id` int(11) NOT NULL,
                            `name` varchar(20) NOT NULL,
                            PRIMARY KEY (`id`)
                        )''')
    await conn.executescript("""
    insert into student(id, name) values(1,'1');
    insert into student(id, name) values(2,'2');
    """)
    cursor = await conn.execute('SELECT * FROM student')
    index = 1
    async for resp in cursor:
        assert resp == (index, str(index))
        index += 1

    # resp = yield from cursor.fetchone()
    # while resp:
    #     resp = yield from cursor.fetchone()
    # assert index == 3


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
    assert not conn.closed
    yield from conn.close()
    yield from conn.close()
    assert conn.closed

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
    yield from cur.close()
    yield from cur.close()
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
async def test_connect_context_manager(loop, db):
    """
    上下文支持
    """
    async with aiosqlite3.connect(db, loop=loop) as conn:
        assert not conn.closed
    assert conn.closed

@pytest.mark.asyncio
async def test_connect_check_same_thread(loop, db):
    """
    测试同步
    """
    async with aiosqlite3.connect(db, loop=loop, check_same_thread=True) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT 42;')
            res = await cursor.fetchone()
            assert res == (42,)
    conn = await aiosqlite3.connect(db, loop=loop, check_same_thread=True)
    assert conn._thread
    await conn.close()
    assert conn._thread is None
    with pytest.raises(TypeError):
        cursor = await conn.execute('SELECT 42;')

@pytest.mark.asyncio
@asyncio.coroutine
def test_connect_execute_error(db, loop):
    """
    测试错误
    """
    conn = yield from aiosqlite3.connect(db, loop=loop, check_same_thread=True)
    with pytest.raises(aiosqlite3.OperationalError):
        yield from conn.execute('sdfd')

# def test_connect_context_sync_manager(conn):
#     """
#     测试普通上下文
#     """
#     assert not conn.closed
#     with conn:
#         pass
#     assert conn.closed

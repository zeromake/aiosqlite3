import asyncio
import pytest

import aiosqlite3
from tests.utils import PY_35
from sqlite3 import Connection, Cursor


def test_cursor(loop, conn, cursor):
    """
    测试cursor属性
    """
    assert not cursor.closed
    assert cursor.loop == loop
    assert cursor.connection == conn
    assert isinstance(cursor.native_connection, Connection)
    assert isinstance(cursor.native_cursor, Cursor)
    assert cursor.arraysize == 1
    cursor.arraysize = 2
    assert cursor.arraysize == 2
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
    assert cursor.description is None

@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchone(cursor):
    """
    测试获取一条数据
    """
    yield from cursor.execute('SELECT 42;')
    res = yield from cursor.fetchone()
    assert res == (42,)

@pytest.mark.asyncio
@asyncio.coroutine
def test_cursor_executemany(cursor):
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
    yield from cursor.execute(create_table_sql)
    yield from cursor.executemany('insert into student(id, name) values(?,?)', char_generator())
    yield from cursor.execute('SELECT * FROM student')
    assert cursor.rowcount == -1
    resp = yield from cursor.fetchone()
    index = 1
    while resp:
        assert resp == (index, str(index))
        resp = yield from cursor.fetchone()
        index += 1
    assert index == 3

@pytest.mark.asyncio
async def test_cursor_executescript(cursor):
    """
    测试executescript
    """
    await cursor.execute(
        '''
        CREATE TABLE `student` (
        `id` int(11) NOT NULL,
        `name` varchar(20) NOT NULL,
        PRIMARY KEY (`id`)
        );
        ''')
    await cursor.executescript(
        """
        insert into student(id, name) values(1,'1');
        insert into student(id, name) values(2,'2');
        """
    )
    await cursor.execute('SELECT * FROM student')
    index = 1
    async for resp in cursor:
        print(resp)
        assert resp == (index, str(index))
        index += 1

import asyncio
import aiosqlite3
from aiosqlite3 import Cursor
from unittest import mock
import pytest
import logging

sa = pytest.importorskip("aiosqlite3.sa")

from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.schema import DropTable, CreateTable
from sqlalchemy.sql.ddl import DDLElement

meta = MetaData()
tbl = Table(
    'sa_tbl',
    meta,
    Column(
        'id',
        Integer,
        nullable=False,
        primary_key=True
    ),
    Column(
        'name',
        String(255)
    )
)
@asyncio.coroutine
def async_res_list(cursor):
    res = []
    index = 0
    while True:
        if index > 2000:
            return res
        data = yield from cursor.fetchone()
        if data is None:
            break
        res.append(data)
        index += 1
    return res

@pytest.yield_fixture
def connect(make_conn):
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_conn(**kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute(
            "DROP TABLE IF EXISTS sa_tbl"
        )
        yield from cur.execute(
            "CREATE TABLE sa_tbl "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255))"
        )
        yield from cur.execute(
            "INSERT INTO sa_tbl (name)"
            "VALUES ('first')"
        )
        cur.close()

        engine = mock.Mock(from_spec=sa.engine.Engine)
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)
    yield go

@pytest.yield_fixture()
def create_pool(loop, db):
    pool = None

    @asyncio.coroutine
    def go(*, no_loop=False, **kwargs):
        nonlocal pool
        params = {'database': db}
        params.update(kwargs)
        pool = yield from aiosqlite3.create_pool(loop=loop, **params)
        return pool
    yield go

    if pool is not None:
        pool.terminate()

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_text_select(connect):
    conn = yield from connect()
    res = yield from conn.execute("SELECT * FROM sa_tbl;")
    assert isinstance(res.cursor, Cursor)
    assert ('id', 'name') == res.keys()
    rows = yield from async_res_list(res)

    # rows = []
    # while True:
    #     data = await res.fetchone()
    #     if data is None:
    #         break
    #     rows.append(data)
    # rows = [r for r in res]
    assert res.closed
    assert res.cursor is None
    assert len(rows) == 1
    row = rows[0]
    assert row[0] is 1
    assert row['id'] is 1
    assert row.id is 1
    assert row[1] == 'first'
    assert row['name'] == 'first'
    assert row.name == 'first'

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_select(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.select())
    assert isinstance(res.cursor, Cursor)
    assert ('id', 'name') == res.keys()
    rows = yield from async_res_list(res)
    assert res.closed
    assert res.cursor is None
    assert res.returns_rows

    assert len(rows) == 1
    row = rows[0]
    assert row[0] == 1
    assert row['id'] == 1
    assert row.id == 1
    assert row[1] == 'first'
    assert row['name'] == 'first'
    assert row.name == 'first'

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_insert_with_dict(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), {"id": 2, "name": "second"})

    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_insert_with_tuple(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), (2, "second"))

    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    assert len(rows) == 2
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_insert_with_dict(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), {"id": 2, "name": "second"})

    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_insert_with_tuple(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), (2, "second"))

    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_insert_named_params(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), id=2, name="second")

    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]

@pytest.mark.asyncio
@asyncio.coroutine
def test_execute_sa_insert_positional_params(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), 2, "second")

    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@pytest.mark.asyncio
@asyncio.coroutine
def test_scalar(connect):
    conn = yield from connect()
    res = yield from conn.scalar(tbl.count())
    assert 1, res


@pytest.mark.asyncio
@asyncio.coroutine
def test_scalar_None(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.delete())
    res = yield from conn.scalar(tbl.select())
    assert res is None

@pytest.mark.asyncio
@asyncio.coroutine
def test_row_proxy(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.select())
    rows = yield from async_res_list(res)
    row = rows[0]
    row2 = yield from (yield from conn.execute(tbl.select())).first()
    assert 2 == len(row)
    assert ['id', 'name'] == list(row)
    assert 'id' in row
    assert 'unknown' not in row
    assert 'first' == row.name
    assert 'first' == row[tbl.c.name]
    with pytest.raises(AttributeError):
        row.unknown
    assert "(1, 'first')" == repr(row)
    assert (1, 'first') == row.as_tuple()
    assert (555, 'other') != row.as_tuple()
    assert row2 == row
    assert not (row2 != row)
    assert 5 != row

@pytest.mark.asyncio
@asyncio.coroutine
def test_insert(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.insert().values(name='second'))
    yield from conn.commit()
    assert () == res.keys()
    assert 1 == res.rowcount
    assert not res.returns_rows
    with pytest.raises(sa.exc.ResourceClosedError):
        rows = yield from async_res_list(res)
    
@pytest.mark.asyncio
@asyncio.coroutine
def test_raw_insert(connect):
    conn = yield from connect()
    yield from conn.execute(
        "INSERT INTO sa_tbl (name) VALUES ('third')")
    res = yield from conn.execute(tbl.select())
    assert -1 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows
    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert 2 == rows[1].id


@pytest.mark.asyncio
@asyncio.coroutine
def test_raw_insert_with_params(connect):
    conn = yield from connect()
    res = yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (?, ?)",
        2, 'third')
    res = yield from conn.execute(tbl.select())
    assert -1 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert 2 == rows[1].id

@pytest.mark.asyncio
@asyncio.coroutine
def test_raw_insert_with_params_dict(connect):
    conn = yield from connect()
    res = yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (:id, :name)",
        {'id': 2, 'name': 'third'})
    res = yield from conn.execute(tbl.select())
    assert -1 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert 2 == rows[1].id


@pytest.mark.asyncio
@asyncio.coroutine
def test_raw_insert_with_named_params(connect):
    conn = yield from connect()
    res = yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (:id, :name)",
        id=2, name='third')
    res = yield from conn.execute(tbl.select())
    assert -1 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = yield from async_res_list(res)
    assert 2 == len(rows)
    assert 2 == rows[1].id

@pytest.mark.asyncio
@asyncio.coroutine
def test_raw_insert_with_executemany(connect):
    conn = yield from connect()
    # with pytest.raises(sa.ArgumentError):
    yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (:id, :name)",
        [(2, 'third'), (3, 'forth')]
    )

@pytest.mark.asyncio
@asyncio.coroutine
def test_delete(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete().where(tbl.c.id == 1))
    assert () == res.keys()
    assert 1 == res.rowcount
    assert not res.returns_rows
    assert res.closed
    assert res.cursor is None

@pytest.mark.asyncio
@asyncio.coroutine
def test_double_close(connect):
    conn = yield from connect()
    res = yield from conn.execute("SELECT 1")
    yield from res.close()
    assert res.closed
    assert res.cursor is None
    yield from res.close()
    assert res.closed
    assert res.cursor is None


@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchall(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchall()
    assert 2 == len(rows)
    assert res.closed
    assert res.returns_rows
    assert [(1, 'first') == (2, 'second')], rows

@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchall_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    yield from res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchall()

@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchall_not_returns_rows(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchall()

@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchone_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    yield from res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchone()

@pytest.mark.asyncio
@asyncio.coroutine
def test_first_not_returns_rows(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        yield from res.first()


@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchmany(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first')] == rows

@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchmany_with_size(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchmany(100)
    assert 2 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first') == (2, 'second')], rows


@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchmany_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    yield from res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()

@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchmany_with_size_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    yield from res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany(5555)


@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchmany_not_returns_rows(connect):
    conn = yield from connect()
    assert not conn.closed
    res = yield from conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()
    yield from conn.close()
    yield from conn.close()
    assert conn.closed


@pytest.mark.asyncio
@asyncio.coroutine
def test_fetchmany_close_after_last_read(connect):
    conn = yield from connect()

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first')] == rows
    rows2 = yield from res.fetchmany()
    assert 0 == len(rows2)
    assert res.closed


@pytest.mark.asyncio
async def test_sa_connection_context(make_engine):
    engine = await make_engine()
    async with engine.acquire() as conn:
        async with conn.execute("SELECT 42") as res:
            async for row in res:
                assert row == (42,)
@pytest.mark.asyncio
@asyncio.coroutine
def test_sa_connection_parameters(connect):
    conn = yield from connect()

    with pytest.raises(sa.exc.ArgumentError):
        yield from conn.execute([])


@pytest.mark.asyncio
@asyncio.coroutine
def test_create_table(connect):
    conn = yield from connect()
    res = yield from conn.execute(DropTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()

    with pytest.raises(aiosqlite3.OperationalError):
        yield from conn.execute("SELECT * FROM sa_tbl")

    res = yield from conn.execute(CreateTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()

    res = yield from conn.execute("SELECT * FROM sa_tbl")
    data = yield from async_res_list(res)
    assert 0 == len(data)

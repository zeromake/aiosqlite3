import asyncio
import tempfile
from unittest import mock
import pytest
sa = pytest.importorskip("aiosqlite3.sa")

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table(
    'sa_tbl2',
    meta,
    Column(
        'id',
        Integer,
        nullable=False,
        primary_key=True
    ),
    Column('name', String(255))
)
db = tempfile.mktemp('.db')

@pytest.yield_fixture
def sa_connect(make_conn):
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_conn(database=db, **kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS sa_tbl2")
        yield from cur.execute(
            "CREATE TABLE sa_tbl2 "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255))"
        )
        yield from cur.execute(
            "INSERT INTO sa_tbl2 (name)"
            "VALUES ('first')"
        )
        yield from cur.close()
        yield from conn.commit()
        engine = mock.Mock(from_spec=sa.engine.Engine)
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)
    yield go

@pytest.yield_fixture
def xa_connect(sa_connect):
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from sa_connect(**kwargs)
        return conn
    yield go


@pytest.mark.asyncio
@asyncio.coroutine
def test_without_transactions(sa_connect):
    conn1 = yield from sa_connect()
    conn2 = yield from sa_connect()
    res1 = yield from conn1.scalar(tbl.count())
    assert 1 == res1

    yield from conn2.execute(tbl.delete())
    yield from conn2.commit()

    res2 = yield from conn1.scalar(tbl.count())
    assert 0 == res2

@pytest.mark.asyncio
@asyncio.coroutine
def test_connection_attr(sa_connect):
    conn = yield from sa_connect()
    tr = yield from conn.begin()
    assert tr.connection is conn

@pytest.mark.asyncio
@asyncio.coroutine
def test_root_transaction(sa_connect):
    conn1 = yield from sa_connect()
    conn2 = yield from sa_connect()

    tr = yield from conn1.begin()
    assert tr.is_active
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from tr.commit()

    assert not tr.is_active
    assert not conn1.in_transaction
    res2 = yield from conn2.scalar(tbl.count())
    assert 0 == res2


@pytest.mark.asyncio
@asyncio.coroutine
def test_root_transaction_rollback(sa_connect):
    conn1 = yield from sa_connect()
    conn2 = yield from sa_connect()

    tr = yield from conn1.begin()
    assert tr.is_active
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from tr.rollback()

    assert not tr.is_active
    res2 = yield from conn2.scalar(tbl.count())
    assert 1 == res2

@pytest.mark.asyncio
@asyncio.coroutine
def test_root_transaction_close(sa_connect):
    conn1 = yield from sa_connect()
    conn2 = yield from sa_connect()

    tr = yield from conn1.begin()
    assert tr.is_active
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from tr.close()

    assert not tr.is_active
    res2 = yield from conn2.scalar(tbl.count())
    assert 1 == res2


@pytest.mark.asyncio
@asyncio.coroutine
def test_root_transaction_commit_inactive(sa_connect):
    conn = yield from sa_connect()
    tr = yield from conn.begin()
    assert tr.is_active
    yield from tr.commit()
    assert not tr.is_active
    with pytest.raises(sa.InvalidRequestError):
        yield from tr.commit()

@pytest.mark.asyncio
@asyncio.coroutine
def test_root_transaction_rollback_inactive(sa_connect):
    conn = yield from sa_connect()
    tr = yield from conn.begin()
    assert tr.is_active
    yield from tr.rollback()
    assert not tr.is_active
    yield from tr.rollback()
    assert not tr.is_active


@pytest.mark.asyncio
@asyncio.coroutine
def test_root_transaction_double_close(sa_connect):
    conn = yield from sa_connect()
    tr = yield from conn.begin()
    assert tr.is_active
    yield from tr.close()
    assert not tr.is_active
    yield from tr.close()
    assert not tr.is_active

@pytest.mark.asyncio
@asyncio.coroutine
def test_inner_transaction_commit(sa_connect):
    conn = yield from sa_connect()
    tr1 = yield from conn.begin()
    tr2 = yield from conn.begin()
    assert tr2.is_active

    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active


@pytest.mark.asyncio
@asyncio.coroutine
def test_rollback_on_connection_close(sa_connect):
    conn1 = yield from sa_connect()
    conn2 = yield from sa_connect()

    tr = yield from conn1.begin()
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from conn1.close()

    res2 = yield from conn2.scalar(tbl.count())
    assert 1 == res2
    del tr

@pytest.mark.asyncio
@asyncio.coroutine
def test_inner_transaction_rollback(sa_connect):
    conn = yield from sa_connect()
    tr1 = yield from conn.begin()
    tr2 = yield from conn.begin()
    assert tr2.is_active
    yield from conn.execute(tbl.insert().values(name='aaaa'))

    yield from tr2.rollback()
    assert not tr2.is_active
    assert not tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 1 == res


@pytest.mark.asyncio
@asyncio.coroutine
def test_inner_transaction_close(sa_connect):
    conn = yield from sa_connect()
    res = yield from conn.scalar(tbl.count())
    assert 1 == res
    tr1 = yield from conn.begin()
    tr2 = yield from conn.begin()
    assert tr2.is_active
    yield from conn.execute(tbl.insert().values(name='aaaa'))

    yield from tr2.close()
    assert not tr2.is_active
    assert tr1.is_active
    yield from tr1.commit()
    res = yield from conn.scalar(tbl.count())
    assert 2 == res

@pytest.mark.asyncio
@asyncio.coroutine
def test_nested_transaction_commit(sa_connect):
    conn = yield from sa_connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()
    assert tr1.is_active
    assert tr2.is_active

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res

    yield from tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res

@pytest.mark.asyncio
@asyncio.coroutine
def test_nested_transaction_commit_twice(sa_connect):
    conn = yield from sa_connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res

    yield from tr1.close()

@pytest.mark.asyncio
@asyncio.coroutine
def test_nested_transaction_rollback(sa_connect):
    conn = yield from sa_connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()
    assert tr1.is_active
    assert tr2.is_active

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 1 == res

    yield from tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 1 == res

@pytest.mark.asyncio
@asyncio.coroutine
def test_nested_transaction_rollback_twice(sa_connect):
    conn = yield from sa_connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr1.commit()
    res = yield from conn.scalar(tbl.count())
    assert 1 == res

@pytest.mark.asyncio
async def test_transaction_context(sa_connect):
    conn = await sa_connect()
    res = await conn.scalar(tbl.count())
    assert 1 == res
    async with conn.begin() as tr1:
        await conn.execute(tbl.insert().values(name='aaaa'))
        assert tr1.is_active
    res = await conn.scalar(tbl.count())
    assert 2 == res

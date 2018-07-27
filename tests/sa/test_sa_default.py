import asyncio
import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable

meta = sa.MetaData()
tbl = sa.Table(
    'sa_tbl4',
    meta,
    sa.Column('id', sa.Integer, primary_key=True, nullable=False),
    sa.Column('name', sa.String(255), nullable=False, default='default test'),
    sa.Column('count', sa.Integer, default=100, nullable=None),
    sa.Column('date', sa.DateTime(), default=datetime.datetime.now),
    sa.Column('count_str', sa.Integer, default=len('abcdef')),
    sa.Column('is_active', sa.Boolean, default=True),
    sqlite_autoincrement=True
)

@pytest.fixture
def engine(make_engine, loop):
    @asyncio.coroutine
    def start():
        engine = yield from make_engine()
        with (yield from engine) as conn:
            yield from conn.execute('DROP TABLE IF EXISTS sa_tbl4')
            sql = CreateTable(tbl)
            yield from conn.execute(sql)
            engine.release(conn)
        return engine

    return loop.run_until_complete(start())

@pytest.mark.asyncio
@asyncio.coroutine
def test_default_fields(engine):
    with (yield from engine) as conn:
        yield from conn.execute(tbl.insert().values())
        res = yield from conn.execute(tbl.select())
        row = yield from res.fetchone()
        assert row.count == 100
        assert row.id == 1
        assert row.count_str == 6
        assert row.name == 'default test'
        assert row.is_active == 1
        assert isinstance(row.date, datetime.datetime)
        yield from res.close()

@pytest.mark.asyncio
@asyncio.coroutine
def test_default_fields_isnull(engine):
    with (yield from engine) as conn:
        yield from conn.execute(tbl.insert().values(
            is_active=False,
            date=None,
        ))
        res = yield from conn.execute(tbl.select())
        row = yield from res.fetchone()
        assert row.count == 100
        assert row.id == 1
        assert row.count_str == 6
        assert row.name == 'default test'
        assert row.is_active == 0
        assert row.date is None
        yield from res.close()

@pytest.mark.asyncio
@asyncio.coroutine
def test_default_fields_edit(engine):
    with (yield from engine) as conn:
        date = datetime.datetime.now()
        yield from conn.execute(tbl.insert().values(
            name='edit name',
            is_active=False,
            date=date,
            count=1,
        ))

        res = yield from conn.execute(tbl.select())
        row = yield from res.fetchone()
        assert row.count == 1
        assert row.id == 1
        assert row.count_str == 6
        assert row.name == 'edit name'
        assert row.is_active == 0
        assert row.date == date
        yield from res.close()


# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/connection.py
import asyncio
import weakref

from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.ddl import DDLElement

from . import exc
from .result import create_result_proxy
from .transaction import (
    RootTransaction,
    Transaction,
    NestedTransaction
)
from ..utils import (
    PY_35,
    _TransactionContextManager,
    _SAConnectionContextManager
)


class SAConnection:

    def __init__(self, connection, engine):
        self._connection = connection
        self._transaction = None
        self._savepoint_seq = 0
        self._weak_results = weakref.WeakSet()
        self._engine = engine
        self._dialect = engine.dialect

    def execute(self, query, *multiparams, **params):
        """
        Executes a SQL query with optional parameters.

        query - a SQL query string or any sqlalchemy expression.

        *multiparams/**params - represent bound parameter values to be
        used in the execution.  Typically, the format is a dictionary
        passed to *multiparams:

            yield from conn.execute(
                table.insert(),
                {"id":1, "value":"v1"},
            )

        ...or individual key/values interpreted by **params::

            yield from conn.execute(
                table.insert(), id=1, value="v1"
            )

        In the case that a plain SQL string is passed, a tuple or
        individual values in \*multiparams may be passed::

            yield from conn.execute(
                "INSERT INTO table (id, value) VALUES (%d, %s)",
                (1, "v1")
            )

            yield from conn.execute(
                "INSERT INTO table (id, value) VALUES (%s, %s)",
                1, "v1"
            )

        Returns ResultProxy instance with results of SQL query
        execution.

        """
        coro = self._execute(query, *multiparams, **params)
        return _SAConnectionContextManager(coro)

    def _base_params(self, query, dp, compiled, is_update):
        if dp and isinstance(dp, (list, tuple)):
            if is_update:
                dp = {c.key: pval for c, pval in zip(query.table.c, dp)}
            else:
                # pragma: no cover
                raise exc.ArgumentError(
                    "Don't mix sqlalchemy SELECT "
                    "clause with positional "
                    "parameters"
                )
        compiled_params = compiled.construct_params(dp)
        processors = compiled._bind_processors
        params = [{
            key: (
                processors[key](compiled_params[key])
                if key in processors else compiled_params[key]
            )
            for key in compiled_params
        }]
        processed_params = self._dialect.execute_sequence_format(params)
        return processed_params[0]

    @asyncio.coroutine
    def _executemany(self, query, dps, cursor):
        result_map = None
        if isinstance(query, str):
            yield from cursor.executemany(query, dps)
        elif isinstance(query, DDLElement):
            raise exc.ArgumentError(
                "Don't mix sqlalchemy DDL clause "
                "and execution with parameters"
            )
        elif isinstance(query, ClauseElement):
            compiled = query.compile(dialect=self._dialect)
            is_update = isinstance(query, UpdateBase)
            params = [self._base_params(
                query,
                dp,
                compiled,
                is_update,
            ) for dp in dps]
            yield from cursor.executemany(str(compiled), params)
            result_map = compiled._result_columns
        else:
            raise exc.ArgumentError(
                "sql statement should be str or "
                "SQLAlchemy data "
                "selection/modification clause"
            )
        ret = yield from create_result_proxy(
            self,
            cursor,
            self._dialect,
            result_map
        )
        self._weak_results.add(ret)
        return ret

    @asyncio.coroutine
    def _execute(self, query, *multiparams, **params):
        """
        execute or executemany
        """
        cursor = yield from self._connection.cursor()
        dp = _distill_params(multiparams, params)
        if len(dp) > 1:
            return (yield from self._executemany(query, dp, cursor))
            # raise exc.ArgumentError("aiosqlite3 doesn't support executemany")
        elif dp:
            dp = dp[0]
    
        result_map = None
        if isinstance(query, str):
            yield from cursor.execute(query, dp)
        elif isinstance(query, ClauseElement):
            compiled = query.compile(dialect=self._dialect)
            if not isinstance(query, DDLElement):
                params = self._base_params(query, dp, compiled, isinstance(query, UpdateBase))
                result_map = compiled._result_columns
            else:
                if dp:
                    raise exc.ArgumentError(
                        "Don't mix sqlalchemy DDL clause "
                        "and execution with parameters"
                    )
                params = compiled.construct_params()
            yield from cursor.execute(str(compiled), params)
        else:
            raise exc.ArgumentError(
                "sql statement should be str or "
                "SQLAlchemy data "
                "selection/modification clause"
            )
        ret = yield from create_result_proxy(
            self,
            cursor,
            self._dialect,
            result_map
        )
        self._weak_results.add(ret)
        return ret

    @asyncio.coroutine
    def scalar(self, query, *multiparams, **params):
        """
        Executes a SQL query and returns a scalar value.
        """
        res = yield from self.execute(query, *multiparams, **params)
        return (yield from res.scalar())

    @property
    def closed(self):
        """The readonly property that returns True if connections is closed."""
        return self._connection is None or self._connection.closed

    @property
    def connection(self):
        return self._connection

    def begin(self):
        """Begin a transaction and return a transaction handle.

        The returned object is an instance of Transaction.  This
        object represents the "scope" of the transaction, which
        completes when either the .rollback or .commit method is
        called.

        Nested calls to .begin on the same SAConnection instance will
        return new Transaction objects that represent an emulated
        transaction within the scope of the enclosing transaction,
        that is::

            trans = yield from conn.begin()   # outermost transaction
            trans2 = yield from conn.begin()  # "nested"
            yield from trans2.commit()        # does nothing
            yield from trans.commit()         # actually commits

        Calls to .commit only have an effect when invoked via the
        outermost Transaction object, though the .rollback method of
        any of the Transaction objects will roll back the transaction.

        See also:
          .begin_nested - use a SAVEPOINT
          .begin_twophase - use a two phase/XA transaction

        """
        coro = self._begin()
        return _TransactionContextManager(coro)

    @asyncio.coroutine
    def _begin(self):
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            yield from self._begin_impl()
            return self._transaction
        else:
            return Transaction(self, self._transaction)

    @asyncio.coroutine
    def _begin_impl(self):
        yield from self._connection.execute('BEGIN TRANSACTION')

    @asyncio.coroutine
    def commit(self):
        """
        commit
        """
        yield from self._commit_impl()

    @asyncio.coroutine
    def _commit_impl(self):
        try:
            yield from self._connection.commit()
        finally:
            self._transaction = None

    @asyncio.coroutine
    def _rollback_impl(self):
        try:
            yield from self._connection.rollback()
        finally:
            self._transaction = None

    @asyncio.coroutine
    def begin_nested(self):
        """Begin a nested transaction and return a transaction handle.

        The returned object is an instance of :class:`.NestedTransaction`.

        Nested transactions require SAVEPOINT support in the
        underlying database.  Any transaction in the hierarchy may
        .commit() and .rollback(), however the outermost transaction
        still controls the overall .commit() or .rollback() of the
        transaction of a whole.
        """
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            yield from self._begin_impl()
        else:
            self._transaction = NestedTransaction(self, self._transaction)
            self._transaction._savepoint = yield from self._savepoint_impl()
        return self._transaction

    @asyncio.coroutine
    def _savepoint_impl(self, name=None):
        self._savepoint_seq += 1
        name = 'aiosqlite3_sa_savepoint_%s' % self._savepoint_seq

        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('SAVEPOINT ' + name)
            return name
        finally:
            yield from cur.close()

    @asyncio.coroutine
    def _rollback_to_savepoint_impl(self, name, parent):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('ROLLBACK TO SAVEPOINT ' + name)
        finally:
            yield from cur.close()
        self._transaction = parent

    @asyncio.coroutine
    def _release_savepoint_impl(self, name, parent):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('RELEASE SAVEPOINT ' + name)
        finally:
            yield from cur.close()
        self._transaction = parent

    @property
    def in_transaction(self):
        """Return True if a transaction is in progress."""
        return self._transaction is not None and self._transaction.is_active

    @asyncio.coroutine
    def close(self):
        """Close this SAConnection.

        This results in a release of the underlying database
        resources, that is, the underlying connection referenced
        internally. The underlying connection is typically restored
        back to the connection-holding Pool referenced by the Engine
        that produced this SAConnection. Any transactional state
        present on the underlying connection is also unconditionally
        released via calling Transaction.rollback() method.

        After .close() is called, the SAConnection is permanently in a
        closed state, and will allow no further operations.
        """
        if self._connection is None:
            return

        if self._transaction is not None:
            yield from self._transaction.rollback()
            self._transaction = None
        # don't close underlying connection, it can be reused by pool
        # conn.close()
        self._engine.release(self)
        self._connection = None
        self._engine = None

    def __del__(self):
        """
        清理引用
        """
        self._connection = None
        self._engine = None
        self._transaction = None
        self._weak_results = None
        self._dialect = None

    if PY_35:
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            yield from self.close()
    else:
        # pragma: no cover
        pass


def _distill_params(multiparams, params):
    """Given arguments from the calling form *multiparams, **params,
    return a list of bind parameter structures, usually a list of
    dictionaries.

    In the case of 'raw' execution which accepts positional parameters,
    it may be a list of tuples or lists.

    """

    if not multiparams:
        if params:
            return [params]
        else:
            return []
    elif len(multiparams) == 1:
        zero = multiparams[0]
        if isinstance(zero, (list, tuple)):
            if not zero or hasattr(zero[0], '__iter__') and \
                    not hasattr(zero[0], 'strip'):
                # execute(stmt, [{}, {}, {}, ...])
                # execute(stmt, [(), (), (), ...])
                return zero
            else:
                # execute(stmt, ("value", "value"))
                return [zero]
        elif hasattr(zero, 'keys'):
            # execute(stmt, {"key":"value"})
            return [zero]
        else:
            # execute(stmt, "value")
            return [[zero]]
    else:
        if (hasattr(multiparams[0], '__iter__') and
                not hasattr(multiparams[0], 'strip')):
            return multiparams
        else:
            return [multiparams]

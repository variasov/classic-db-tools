import os

import pytest

pymssql = pytest.importorskip('pymssql')

from classic.db_tools import Engine  # noqa: E402
from classic.db_tools.pool import ConnectionPool, ConnectionLimitError  # noqa: E402
from classic.db_tools.backends.pymssql import PyMSSQLConnectionValidator  # noqa: E402


def connect():
    return pymssql.connect(
        server=os.environ['MSSQL_HOST'],
        user=os.environ['MSSQL_USER'],
        password=os.environ['MSSQL_PASSWORD'],
        port=int(os.environ.get('MSSQL_PORT', '1433')),
        database=os.environ.get('MSSQL_DATABASE', 'master'),
        autocommit=False,
    )


@pytest.fixture
def mssql_engine():
    return Engine(pymssql, connect)


class TestPyMSSQLConnectionValidator:

    def test_validate_returns_true_for_live_connection(self):
        conn = connect()
        validator = PyMSSQLConnectionValidator()
        assert validator.validate(conn) is True
        conn.close()

    def test_validate_returns_false_for_bad_connection(self):
        class FakeConn:
            def cursor(self):
                raise Exception('connection is dead')

        validator = PyMSSQLConnectionValidator()
        assert validator.validate(FakeConn()) is False

    def test_before_release_live_connection(self):
        conn = connect()
        validator = PyMSSQLConnectionValidator()
        assert validator.before_release(conn) is True
        conn.close()

    def test_before_release_bad_connection(self):
        class FakeConn:
            def rollback(self):
                raise Exception('already closed')

        validator = PyMSSQLConnectionValidator()
        assert validator.before_release(FakeConn()) is False


class TestPyMSSQLConnectionPool:

    def test_default_pool_on_engine(self, mssql_engine):
        assert mssql_engine._pool is not None
        assert isinstance(mssql_engine._pool, ConnectionPool)

    def test_connection_limit(self):
        pool = ConnectionPool(pymssql, connect, limit=1, timeout=0.1)
        conn1 = pool.acquire()
        try:
            with pytest.raises(ConnectionLimitError):
                pool.acquire()
        finally:
            pool.release(conn1)

    def test_validator_auto_selected(self):
        pool = ConnectionPool(pymssql, connect, validator='auto')
        assert pool.validate is not None
        assert pool.before_release is not None

    def test_validator_disabled(self):
        pool = ConnectionPool(pymssql, connect, validator=None)
        assert pool.validate is None
        assert pool.before_release is None

    def test_before_release_rolls_back_dirty_connection(self):
        pool = ConnectionPool(pymssql, connect)
        conn = pool.acquire()
        try:
            cursor = conn.cursor()
            cursor.execute(
                'CREATE TABLE #_pool_dirty_test (id INT)'
            )
            cursor.execute('INSERT INTO #_pool_dirty_test (id) VALUES (1)')
            cursor.close()
            reuse = pool.before_release(conn)
            assert reuse is True
        finally:
            pool.release(conn)


class TestPyMSSQLTransactionIsolation:

    def test_isolation_level_serializable(self, mssql_engine):
        with mssql_engine.transaction(level='serializable'):
            result = mssql_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_isolation_level_read_uncommitted(self, mssql_engine):
        with mssql_engine.transaction(level='read uncommitted'):
            result = mssql_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_mismatched_params_raises(self, mssql_engine):
        with mssql_engine.transaction(level='serializable'):
            with pytest.raises(AssertionError):
                with mssql_engine.transaction():
                    mssql_engine.query('SELECT 1', static=True).execute()


class TestPyMSSQLSavepoints:

    def test_start_and_release_savepoint(self, mssql_engine):
        with mssql_engine.conn():
            with mssql_engine.transaction():
                mssql_engine.query(
                    'CREATE TABLE #_sp_test (id INT)', static=True,
                ).execute()
                with mssql_engine.transaction():
                    mssql_engine.query(
                        'INSERT INTO #_sp_test (id) VALUES (1)', static=True,
                    ).execute()
                count = mssql_engine.query(
                    'SELECT COUNT(*) FROM #_sp_test', static=True,
                ).scalar()
        assert count == 1

    def test_rollback_savepoint(self, mssql_engine):
        with mssql_engine.conn():
            with mssql_engine.transaction():
                mssql_engine.query(
                    'CREATE TABLE #_sp_rollback (id INT)', static=True,
                ).execute()
                try:
                    with mssql_engine.transaction():
                        mssql_engine.query(
                            'INSERT INTO #_sp_rollback (id) VALUES (1)',
                            static=True,
                        ).execute()
                        raise RuntimeError('force rollback')
                except RuntimeError:
                    pass
                count = mssql_engine.query(
                    'SELECT COUNT(*) FROM #_sp_rollback', static=True,
                ).scalar()
        assert count == 0


class TestPyMSSQLIntegration:

    def test_full_transaction_commit(self, mssql_engine):
        # #temp tables are connection-scoped in MS SQL — keep everything in one conn()
        with mssql_engine.conn():
            mssql_engine.query(
                'CREATE TABLE #_full_tx (id INT)', static=True,
            ).execute()
            with mssql_engine.transaction():
                mssql_engine.query(
                    'INSERT INTO #_full_tx (id) VALUES (1)', static=True,
                ).execute()
            result = mssql_engine.query(
                'SELECT id FROM #_full_tx', static=True,
            ).scalar()
        assert result == 1

    def test_full_transaction_rollback(self, mssql_engine):
        # DDL must be committed before the rollback-only transaction,
        # otherwise CREATE TABLE is part of the rolled-back implicit TX
        with mssql_engine.conn():
            with mssql_engine.transaction():
                mssql_engine.query(
                    'CREATE TABLE #_full_rb (id INT)', static=True,
                ).execute()
            with mssql_engine.transaction(commit=False):
                mssql_engine.query(
                    'INSERT INTO #_full_rb (id) VALUES (1)', static=True,
                ).execute()
            result = mssql_engine.query(
                'SELECT id FROM #_full_rb', static=True,
            ).scalar()
        assert result is None

    def test_nested_savepoint_commit(self, mssql_engine):
        with mssql_engine.conn():
            mssql_engine.query(
                'CREATE TABLE #_nest_commit (id INT, val VARCHAR(50))',
                static=True,
            ).execute()
            with mssql_engine.transaction():
                mssql_engine.query(
                    "INSERT INTO #_nest_commit (id, val) VALUES (1, 'outer')",
                    static=True,
                ).execute()
                with mssql_engine.transaction():
                    mssql_engine.query(
                        "INSERT INTO #_nest_commit (id, val) VALUES (2, 'inner')",
                        static=True,
                    ).execute()
            result = mssql_engine.query(
                'SELECT COUNT(*) FROM #_nest_commit', static=True,
            ).scalar()
        assert result == 2

    def test_nested_savepoint_rollback(self, mssql_engine):
        with mssql_engine.conn():
            mssql_engine.query(
                'CREATE TABLE #_nest_rb (id INT)', static=True,
            ).execute()
            with mssql_engine.transaction():
                mssql_engine.query(
                    'INSERT INTO #_nest_rb (id) VALUES (1)', static=True,
                ).execute()
                try:
                    with mssql_engine.transaction():
                        mssql_engine.query(
                            'INSERT INTO #_nest_rb (id) VALUES (2)', static=True,
                        ).execute()
                        raise RuntimeError('inner fail')
                except RuntimeError:
                    pass
            result = mssql_engine.query(
                'SELECT COUNT(*) FROM #_nest_rb', static=True,
            ).scalar()
        assert result == 1

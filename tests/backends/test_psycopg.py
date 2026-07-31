import os

import pytest

psycopg = pytest.importorskip('psycopg')

from classic.db_tools import Engine, ConnectionPool  # noqa: E402
from classic.db_tools.pool import ConnectionLimitError  # noqa: E402
from classic.db_tools.backends.psycopg import PsycopgConnectionValidator  # noqa: E402


def connect():
    return psycopg.connect(
        dbname=os.environ.get('PGDBNAME', 'test'),
        user=os.environ.get('PGUSER', 'test'),
        password=os.environ.get('PGPASSWORD', 'test'),
    )


@pytest.fixture
def pg_engine():
    return Engine(psycopg, connect)


class TestPsycopgTransactionParams:

    def test_readonly(self, pg_engine):
        with pg_engine.transaction(readonly=True):
            result = pg_engine.query(
                'SELECT 1 AS a', static=True,
            ).scalar()
            assert result == 1

    def test_isolation_level(self, pg_engine):
        with pg_engine.transaction(level='serializable'):
            result = pg_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_deferrable(self, pg_engine):
        with pg_engine.transaction(readonly=True, deferrable=True):
            result = pg_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_mismatched_params_raises(self, pg_engine):
        with pg_engine.transaction(readonly=True):
            with pytest.raises(AssertionError):
                with pg_engine.transaction():
                    pg_engine.query('SELECT 1', static=True).execute()


class TestPsycopgConnectionPool:

    def test_default_pool_on_engine(self, pg_engine):
        assert pg_engine._pool is not None
        assert isinstance(pg_engine._pool, ConnectionPool)

    def test_acquire_and_release(self, pg_engine):
        conn = pg_engine._pool.acquire()
        assert conn is not None
        pg_engine._pool.release(conn)

    def test_context_manager_wrapped_connection(self, pg_engine):
        with pg_engine._pool.connect() as conn:
            assert conn is not None
            conn.execute('SELECT 1')

    def test_connection_limit(self):
        pool = ConnectionPool(psycopg, connect, limit=1, timeout=0.1)
        conn1 = pool.acquire()
        try:
            with pytest.raises(ConnectionLimitError):
                pool.acquire()
        finally:
            pool.release(conn1)

    def test_validator_auto_selected(self):
        pool = ConnectionPool(psycopg, connect, validator='auto')
        assert pool.validate is not None
        assert pool.before_release is not None

    def test_validator_disabled(self):
        pool = ConnectionPool(psycopg, connect, validator=None)
        assert pool.validate is None
        assert pool.before_release is None

    def test_validator_rejects_bad_connection(self):
        class FakeConn:
            def cursor(self):
                raise Exception('bad')

            def close(self):
                pass

        validator = PsycopgConnectionValidator()
        assert not validator.validate(FakeConn())

    def test_before_release_rolls_back(self):
        pool = ConnectionPool(psycopg, connect)
        conn = pool.acquire()
        try:
            conn.rollback()
            conn.autocommit = False,
            conn.execute(
                'CREATE TEMP TABLE IF NOT EXISTS _tmp_pool_test (id INT)',
            )
            conn.execute('INSERT INTO _tmp_pool_test (id) VALUES (1)')
            reuse = pool.before_release(conn)
            assert reuse is True
        finally:
            pool.release(conn)

    def test_before_release_rejects_closed(self):
        pool = ConnectionPool(psycopg, connect)
        conn = pool.acquire()
        conn.close()
        reuse = pool.before_release(conn)
        assert reuse is False

    def test_unlimited_creates_new_on_empty_queue(self):
        pool = ConnectionPool(psycopg, connect, limit=0)
        conn = pool.acquire()
        try:
            conn2 = pool.acquire()
            pool.release(conn2)
        finally:
            pool.release(conn)

import psycopg
import pytest

from classic.db_tools import ConnectionPool
from classic.db_tools.pool import ConnectionLimitError
from classic.db_tools.backends.psycopg import PsycopgConnectionValidator

from conftest import connect


class TestConnectionPool:

    def test_default_pool_on_engine(self, engine):
        assert engine._pool is not None
        assert isinstance(engine._pool, ConnectionPool)

    def test_acquire_and_release(self, engine):
        conn = engine._pool.acquire()
        assert conn is not None
        engine._pool.release(conn)

    def test_context_manager_wrapped_connection(self, engine):
        with engine._pool.connect() as conn:
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
            conn.autocommit = False
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

    def test_before_release_rejects_unknown_status(self):
        pool = ConnectionPool(psycopg, connect)
        conn = pool.acquire()
        conn.close()
        assert pool.before_release(conn) is False

    def test_unlimited_creates_new_on_empty_queue(self):
        pool = ConnectionPool(psycopg, connect, limit=0)
        conn = pool.acquire()
        try:
            conn2 = pool.acquire()
            pool.release(conn2)
        finally:
            pool.release(conn)

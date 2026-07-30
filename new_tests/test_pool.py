import pytest
import psycopg

from classic.db_tools import ConnectionPool
from classic.db_tools.pool import ConnectionLimitError
from classic.db_tools.backends.psycopg import (
    PsycopgConnectionValidator,
)


class TestConnectionPool:

    def test_default_pool_created(self, engine):
        assert engine._pool is not None

    def test_acquire_and_release(self, engine):
        with engine.transaction():
            conn = engine._pool.acquire()
            assert conn is not None
            engine._pool.release(conn)

    def test_connection_limit(self):
        pool = ConnectionPool(
            psycopg,
            psycopg.connect,
            limit=1,
            timeout=0.1,
        )
        conn1 = pool.acquire()
        with pytest.raises(ConnectionLimitError):
            pool.acquire()
        pool.release(conn1)

    def test_validator_auto_selected(self):
        pool = ConnectionPool(
            psycopg,
            psycopg.connect,
            validator='auto',
        )
        assert pool.validate is not None
        assert pool.before_release is not None

    def test_validator_rejects_bad_conn(self):
        class FakeConn:
            def cursor(self):
                raise Exception('bad')
            def close(self):
                pass

        validator = PsycopgConnectionValidator()
        assert not validator.validate(FakeConn())

    def test_before_release_rolls_back_uncommitted(self):
        pool = ConnectionPool(
            psycopg,
            psycopg.connect,
        )
        conn = pool.acquire()
        from psycopg.pq import TransactionStatus
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        conn.autocommit = False
        conn.execute('CREATE TEMP TABLE _tmp_pool_test (id INT)')
        reuse = pool.before_release(conn)
        assert reuse is True
        pool.release(conn)

    def test_before_release_rejects_closed(self):
        pool = ConnectionPool(
            psycopg,
            psycopg.connect,
        )
        conn = pool.acquire()
        conn.close()
        reuse = pool.before_release(conn)
        assert reuse is False
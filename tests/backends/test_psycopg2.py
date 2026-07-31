import os

import pytest

psycopg2 = pytest.importorskip('psycopg2')

from classic.db_tools import Engine  # noqa: E402
from classic.db_tools.pool import ConnectionPool, ConnectionLimitError  # noqa: E402
from classic.db_tools.backends.psycopg2 import Psycopg2ConnectionValidator  # noqa: E402


def connect():
    return psycopg2.connect(
        host=os.environ['PGHOST'],
        port=int(os.environ.get('PGPORT', '5432')),
        user=os.environ['PGUSER'],
        password=os.environ['PGPASSWORD'],
        dbname=os.environ.get('PGDBNAME', 'postgres'),
    )


@pytest.fixture
def pg2_engine():
    return Engine(psycopg2, connect)


class TestPsycopg2ConnectionValidator:

    def test_validate_returns_true_for_live_connection(self):
        conn = connect()
        validator = Psycopg2ConnectionValidator()
        assert validator.validate(conn) is True
        conn.close()

    def test_validate_returns_false_for_bad_connection(self):
        class FakeConn:
            def cursor(self):
                raise Exception('connection is dead')

        validator = Psycopg2ConnectionValidator()
        assert validator.validate(FakeConn()) is False

    def test_before_release_live_connection(self):
        conn = connect()
        validator = Psycopg2ConnectionValidator()
        assert validator.before_release(conn) is True
        conn.close()

    def test_before_release_bad_connection(self):
        conn = connect()
        conn.close()
        validator = Psycopg2ConnectionValidator()
        assert validator.before_release(conn) is False

    def test_before_release_unknown_status(self):
        import psycopg2.extensions as ext

        class FakeInfo:
            transaction_status = ext.TRANSACTION_STATUS_UNKNOWN

        class FakeConn:
            closed = 0
            info = FakeInfo()

        validator = Psycopg2ConnectionValidator()
        assert validator.before_release(FakeConn()) is False


class TestPsycopg2ConnectionPool:

    def test_default_pool_on_engine(self, pg2_engine):
        assert pg2_engine._pool is not None
        assert isinstance(pg2_engine._pool, ConnectionPool)

    def test_connection_limit(self):
        pool = ConnectionPool(psycopg2, connect, limit=1, timeout=0.1)
        conn1 = pool.acquire()
        try:
            with pytest.raises(ConnectionLimitError):
                pool.acquire()
        finally:
            pool.release(conn1)

    def test_validator_auto_selected(self):
        pool = ConnectionPool(psycopg2, connect, validator='auto')
        assert pool.validate is not None
        assert pool.before_release is not None

    def test_validator_disabled(self):
        pool = ConnectionPool(psycopg2, connect, validator=None)
        assert pool.validate is None
        assert pool.before_release is None

    def test_before_release_rolls_back_dirty_connection(self):
        pool = ConnectionPool(psycopg2, connect)
        conn = pool.acquire()
        try:
            cursor = conn.cursor()
            cursor.execute(
                'CREATE TEMP TABLE IF NOT EXISTS _pool_dirty_test (id INT)',
            )
            cursor.execute('INSERT INTO _pool_dirty_test (id) VALUES (1)')
            cursor.close()
            reuse = pool.before_release(conn)
            assert reuse is True
        finally:
            pool.release(conn)


class TestPsycopg2TransactionIsolation:

    def test_isolation_level_serializable(self, pg2_engine):
        with pg2_engine.transaction(level='serializable'):
            result = pg2_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_isolation_level_read_uncommitted(self, pg2_engine):
        with pg2_engine.transaction(level='read uncommitted'):
            result = pg2_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_mismatched_params_raises(self, pg2_engine):
        with pg2_engine.transaction(level='serializable'):
            with pytest.raises(AssertionError):
                with pg2_engine.transaction():
                    pg2_engine.query('SELECT 1', static=True).execute()


class TestPsycopg2Savepoints:

    def test_start_and_release_savepoint(self, pg2_engine):
        with pg2_engine.conn():
            with pg2_engine.transaction():
                pg2_engine.query(
                    'CREATE TABLE IF NOT EXISTS _sp_test (id INT)', static=True,
                ).execute()
            try:
                with pg2_engine.transaction():
                    with pg2_engine.transaction():
                        pg2_engine.query(
                            'INSERT INTO _sp_test (id) VALUES (1)', static=True,
                        ).execute()
                    count = pg2_engine.query(
                        'SELECT COUNT(*) FROM _sp_test', static=True,
                    ).scalar()
                assert count == 1
            finally:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'DROP TABLE IF EXISTS _sp_test', static=True,
                    ).execute()

    def test_rollback_savepoint(self, pg2_engine):
        with pg2_engine.conn():
            with pg2_engine.transaction():
                pg2_engine.query(
                    'CREATE TABLE IF NOT EXISTS _sp_rollback (id INT)', static=True,
                ).execute()
            try:
                with pg2_engine.transaction():
                    try:
                        with pg2_engine.transaction():
                            pg2_engine.query(
                                'INSERT INTO _sp_rollback (id) VALUES (1)',
                                static=True,
                            ).execute()
                            raise RuntimeError('force rollback')
                    except RuntimeError:
                        pass
                    count = pg2_engine.query(
                        'SELECT COUNT(*) FROM _sp_rollback', static=True,
                    ).scalar()
                assert count == 0
            finally:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'DROP TABLE IF EXISTS _sp_rollback', static=True,
                    ).execute()


class TestPsycopg2Integration:

    def test_full_transaction_commit(self, pg2_engine):
        with pg2_engine.conn():
            with pg2_engine.transaction():
                pg2_engine.query(
                    'CREATE TABLE IF NOT EXISTS _full_tx (id INT)', static=True,
                ).execute()
            try:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'INSERT INTO _full_tx (id) VALUES (1)', static=True,
                    ).execute()
                result = pg2_engine.query(
                    'SELECT id FROM _full_tx', static=True,
                ).scalar()
                assert result == 1
            finally:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'DROP TABLE IF EXISTS _full_tx', static=True,
                    ).execute()

    def test_full_transaction_rollback(self, pg2_engine):
        with pg2_engine.conn():
            with pg2_engine.transaction():
                pg2_engine.query(
                    'CREATE TABLE IF NOT EXISTS _full_rb (id INT)', static=True,
                ).execute()
            try:
                with pg2_engine.transaction(commit=False):
                    pg2_engine.query(
                        'INSERT INTO _full_rb (id) VALUES (1)', static=True,
                    ).execute()
                result = pg2_engine.query(
                    'SELECT id FROM _full_rb', static=True,
                ).scalar()
                assert result is None
            finally:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'DROP TABLE IF EXISTS _full_rb', static=True,
                    ).execute()

    def test_nested_savepoint_commit(self, pg2_engine):
        with pg2_engine.conn():
            with pg2_engine.transaction():
                pg2_engine.query(
                    'CREATE TABLE IF NOT EXISTS _nest_commit (id INT, val VARCHAR(50))',
                    static=True,
                ).execute()
            try:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        "INSERT INTO _nest_commit (id, val) VALUES (1, 'outer')",
                        static=True,
                    ).execute()
                    with pg2_engine.transaction():
                        pg2_engine.query(
                            "INSERT INTO _nest_commit (id, val) VALUES (2, 'inner')",
                            static=True,
                        ).execute()
                result = pg2_engine.query(
                    'SELECT COUNT(*) FROM _nest_commit', static=True,
                ).scalar()
                assert result == 2
            finally:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'DROP TABLE IF EXISTS _nest_commit', static=True,
                    ).execute()

    def test_nested_savepoint_rollback(self, pg2_engine):
        with pg2_engine.conn():
            with pg2_engine.transaction():
                pg2_engine.query(
                    'CREATE TABLE IF NOT EXISTS _nest_rb (id INT)', static=True,
                ).execute()
            try:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'INSERT INTO _nest_rb (id) VALUES (1)', static=True,
                    ).execute()
                    try:
                        with pg2_engine.transaction():
                            pg2_engine.query(
                                'INSERT INTO _nest_rb (id) VALUES (2)', static=True,
                            ).execute()
                            raise RuntimeError('inner fail')
                    except RuntimeError:
                        pass
                result = pg2_engine.query(
                    'SELECT COUNT(*) FROM _nest_rb', static=True,
                ).scalar()
                assert result == 1
            finally:
                with pg2_engine.transaction():
                    pg2_engine.query(
                        'DROP TABLE IF EXISTS _nest_rb', static=True,
                    ).execute()

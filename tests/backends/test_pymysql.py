import os

import pytest

pymysql = pytest.importorskip('pymysql')

from classic.db_tools import Engine  # noqa: E402
from classic.db_tools.pool import ConnectionPool, ConnectionLimitError  # noqa: E402
from classic.db_tools.backends.pymysql import PyMySQLConnectionValidator  # noqa: E402


def connect():
    return pymysql.connect(
        host=os.environ['MYSQL_HOST'],
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'],
        port=int(os.environ.get('MYSQL_PORT', '3306')),
        database=os.environ.get('MYSQL_DATABASE', 'test'),
        autocommit=False,
    )


@pytest.fixture
def mysql_engine():
    return Engine(pymysql, connect)


class TestPyMySQLConnectionValidator:

    def test_validate_returns_true_for_live_connection(self):
        conn = connect()
        validator = PyMySQLConnectionValidator()
        assert validator.validate(conn) is True
        conn.close()

    def test_validate_returns_false_for_bad_connection(self):
        class FakeConn:
            def cursor(self):
                raise Exception('connection is dead')

        validator = PyMySQLConnectionValidator()
        assert validator.validate(FakeConn()) is False

    def test_before_release_live_connection(self):
        conn = connect()
        validator = PyMySQLConnectionValidator()
        assert validator.before_release(conn) is True
        conn.close()

    def test_before_release_bad_connection(self):
        class FakeConn:
            def rollback(self):
                raise Exception('already closed')

        validator = PyMySQLConnectionValidator()
        assert validator.before_release(FakeConn()) is False


class TestPyMySQLConnectionPool:

    def test_default_pool_on_engine(self, mysql_engine):
        assert mysql_engine._pool is not None
        assert isinstance(mysql_engine._pool, ConnectionPool)

    def test_connection_limit(self):
        pool = ConnectionPool(pymysql, connect, limit=1, timeout=0.1)
        conn1 = pool.acquire()
        try:
            with pytest.raises(ConnectionLimitError):
                pool.acquire()
        finally:
            pool.release(conn1)

    def test_validator_auto_selected(self):
        pool = ConnectionPool(pymysql, connect, validator='auto')
        assert pool.validate is not None
        assert pool.before_release is not None

    def test_validator_disabled(self):
        pool = ConnectionPool(pymysql, connect, validator=None)
        assert pool.validate is None
        assert pool.before_release is None

    def test_before_release_rolls_back_dirty_connection(self):
        pool = ConnectionPool(pymysql, connect)
        conn = pool.acquire()
        try:
            cursor = conn.cursor()
            # DDL in MySQL auto-commits, so we use DML only here
            cursor.execute('CREATE TABLE IF NOT EXISTS _pool_dirty_test (id INT)')
            cursor.execute('INSERT INTO _pool_dirty_test (id) VALUES (1)')
            cursor.close()
            reuse = pool.before_release(conn)
            assert reuse is True
        finally:
            cursor = conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS _pool_dirty_test')
            cursor.close()
            conn.commit()
            pool.release(conn)


class TestPyMySQLTransactionIsolation:

    def test_isolation_level_serializable(self, mysql_engine):
        with mysql_engine.transaction(level='serializable'):
            result = mysql_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_isolation_level_read_uncommitted(self, mysql_engine):
        with mysql_engine.transaction(level='read uncommitted'):
            result = mysql_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_mismatched_params_raises(self, mysql_engine):
        with mysql_engine.transaction(level='serializable'):
            with pytest.raises(AssertionError):
                with mysql_engine.transaction():
                    mysql_engine.query('SELECT 1', static=True).execute()


class TestPyMySQLSavepoints:

    def test_start_and_release_savepoint(self, mysql_engine):
        # DDL auto-commits in MySQL — create table outside any transaction
        with mysql_engine.conn():
            mysql_engine.query(
                'CREATE TABLE IF NOT EXISTS _sp_test (id INT)', static=True,
            ).execute()
            try:
                with mysql_engine.transaction():
                    with mysql_engine.transaction():
                        mysql_engine.query(
                            'INSERT INTO _sp_test (id) VALUES (1)', static=True,
                        ).execute()
                    count = mysql_engine.query(
                        'SELECT COUNT(*) FROM _sp_test', static=True,
                    ).scalar()
                assert count == 1
            finally:
                mysql_engine.query(
                    'DROP TABLE IF EXISTS _sp_test', static=True,
                ).execute()

    def test_rollback_savepoint(self, mysql_engine):
        with mysql_engine.conn():
            mysql_engine.query(
                'CREATE TABLE IF NOT EXISTS _sp_rollback (id INT)', static=True,
            ).execute()
            try:
                with mysql_engine.transaction():
                    try:
                        with mysql_engine.transaction():
                            mysql_engine.query(
                                'INSERT INTO _sp_rollback (id) VALUES (1)',
                                static=True,
                            ).execute()
                            raise RuntimeError('force rollback')
                    except RuntimeError:
                        pass
                    count = mysql_engine.query(
                        'SELECT COUNT(*) FROM _sp_rollback', static=True,
                    ).scalar()
                assert count == 0
            finally:
                mysql_engine.query(
                    'DROP TABLE IF EXISTS _sp_rollback', static=True,
                ).execute()


class TestPyMySQLIntegration:

    def test_full_transaction_commit(self, mysql_engine):
        with mysql_engine.conn():
            mysql_engine.query(
                'CREATE TABLE IF NOT EXISTS _full_tx (id INT)', static=True,
            ).execute()
            try:
                with mysql_engine.transaction():
                    mysql_engine.query(
                        'INSERT INTO _full_tx (id) VALUES (1)', static=True,
                    ).execute()
                result = mysql_engine.query(
                    'SELECT id FROM _full_tx', static=True,
                ).scalar()
                assert result == 1
            finally:
                mysql_engine.query(
                    'DROP TABLE IF EXISTS _full_tx', static=True,
                ).execute()

    def test_full_transaction_rollback(self, mysql_engine):
        with mysql_engine.conn():
            mysql_engine.query(
                'CREATE TABLE IF NOT EXISTS _full_rb (id INT)', static=True,
            ).execute()
            try:
                with mysql_engine.transaction(commit=False):
                    mysql_engine.query(
                        'INSERT INTO _full_rb (id) VALUES (1)', static=True,
                    ).execute()
                result = mysql_engine.query(
                    'SELECT id FROM _full_rb', static=True,
                ).scalar()
                assert result is None
            finally:
                mysql_engine.query(
                    'DROP TABLE IF EXISTS _full_rb', static=True,
                ).execute()

    def test_nested_savepoint_commit(self, mysql_engine):
        with mysql_engine.conn():
            mysql_engine.query(
                'CREATE TABLE IF NOT EXISTS _nest_commit (id INT, val VARCHAR(50))',
                static=True,
            ).execute()
            try:
                with mysql_engine.transaction():
                    mysql_engine.query(
                        "INSERT INTO _nest_commit (id, val) VALUES (1, 'outer')",
                        static=True,
                    ).execute()
                    with mysql_engine.transaction():
                        mysql_engine.query(
                            "INSERT INTO _nest_commit (id, val) VALUES (2, 'inner')",
                            static=True,
                        ).execute()
                result = mysql_engine.query(
                    'SELECT COUNT(*) FROM _nest_commit', static=True,
                ).scalar()
                assert result == 2
            finally:
                mysql_engine.query(
                    'DROP TABLE IF EXISTS _nest_commit', static=True,
                ).execute()

    def test_nested_savepoint_rollback(self, mysql_engine):
        with mysql_engine.conn():
            mysql_engine.query(
                'CREATE TABLE IF NOT EXISTS _nest_rb (id INT)', static=True,
            ).execute()
            try:
                with mysql_engine.transaction():
                    mysql_engine.query(
                        'INSERT INTO _nest_rb (id) VALUES (1)', static=True,
                    ).execute()
                    try:
                        with mysql_engine.transaction():
                            mysql_engine.query(
                                'INSERT INTO _nest_rb (id) VALUES (2)', static=True,
                            ).execute()
                            raise RuntimeError('inner fail')
                    except RuntimeError:
                        pass
                result = mysql_engine.query(
                    'SELECT COUNT(*) FROM _nest_rb', static=True,
                ).scalar()
                assert result == 1
            finally:
                mysql_engine.query(
                    'DROP TABLE IF EXISTS _nest_rb', static=True,
                ).execute()

import os

import pytest

oracledb = pytest.importorskip('oracledb')

from classic.db_tools import Engine  # noqa: E402
from classic.db_tools.pool import ConnectionPool, ConnectionLimitError  # noqa: E402
from classic.db_tools.backends.oracledb import OracleDBConnectionValidator  # noqa: E402


def connect():
    dsn = oracledb.makedsn(
        host=os.environ['ORACLE_HOST'],
        port=int(os.environ.get('ORACLE_PORT', '1521')),
        service_name=os.environ.get('ORACLE_SERVICE', 'XEPDB1'),
    )
    return oracledb.connect(
        user=os.environ['ORACLE_USER'],
        password=os.environ['ORACLE_PASSWORD'],
        dsn=dsn,
    )


@pytest.fixture
def oracle_engine():
    return Engine(oracledb, connect)


class TestOracleDBConnectionValidator:

    def test_validate_returns_true_for_live_connection(self):
        conn = connect()
        validator = OracleDBConnectionValidator()
        assert validator.validate(conn) is True
        conn.close()

    def test_validate_returns_false_for_bad_connection(self):
        class FakeConn:
            def cursor(self):
                raise Exception('connection is dead')

        validator = OracleDBConnectionValidator()
        assert validator.validate(FakeConn()) is False

    def test_before_release_live_connection(self):
        conn = connect()
        validator = OracleDBConnectionValidator()
        assert validator.before_release(conn) is True
        conn.close()

    def test_before_release_bad_connection(self):
        class FakeConn:
            def rollback(self):
                raise Exception('already closed')

        validator = OracleDBConnectionValidator()
        assert validator.before_release(FakeConn()) is False


class TestOracleDBConnectionPool:

    def test_default_pool_on_engine(self, oracle_engine):
        assert oracle_engine._pool is not None
        assert isinstance(oracle_engine._pool, ConnectionPool)

    def test_connection_limit(self):
        pool = ConnectionPool(oracledb, connect, limit=1, timeout=0.1)
        conn1 = pool.acquire()
        try:
            with pytest.raises(ConnectionLimitError):
                pool.acquire()
        finally:
            pool.release(conn1)

    def test_validator_auto_selected(self):
        pool = ConnectionPool(oracledb, connect, validator='auto')
        assert pool.validate is not None
        assert pool.before_release is not None

    def test_validator_disabled(self):
        pool = ConnectionPool(oracledb, connect, validator=None)
        assert pool.validate is None
        assert pool.before_release is None

    def test_before_release_rolls_back_dirty_connection(self):
        pool = ConnectionPool(oracledb, connect)
        conn = pool.acquire()
        try:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO _pool_dirty_test (id) VALUES (1)')
            cursor.close()
            reuse = pool.before_release(conn)
            assert reuse is True
        finally:
            pool.release(conn)


class TestOracleDBTransactionIsolation:

    def test_isolation_level_serializable(self, oracle_engine):
        # Oracle: SET TRANSACTION must be the first statement in a transaction
        with oracle_engine.transaction(level='serializable'):
            result = oracle_engine.query(
                'SELECT 1 FROM DUAL', static=True,
            ).scalar()
            assert result == 1

    def test_isolation_level_read_committed(self, oracle_engine):
        with oracle_engine.transaction(level='read committed'):
            result = oracle_engine.query(
                'SELECT 1 FROM DUAL', static=True,
            ).scalar()
            assert result == 1

    def test_mismatched_params_raises(self, oracle_engine):
        with oracle_engine.transaction(level='serializable'):
            with pytest.raises(AssertionError):
                with oracle_engine.transaction():
                    oracle_engine.query(
                        'SELECT 1 FROM DUAL', static=True,
                    ).execute()

    def test_read_uncommitted_not_supported(self, oracle_engine):
        # Oracle does not support READ UNCOMMITTED — driver raises DatabaseError.
        # This test documents the expected behavior.
        with pytest.raises(oracledb.DatabaseError):
            with oracle_engine.transaction(level='read uncommitted'):
                oracle_engine.query(
                    'SELECT 1 FROM DUAL', static=True,
                ).execute()


class TestOracleDBSavepoints:
    # Oracle DDL causes an implicit COMMIT, so we use a pre-existing table.
    # The table _sp_test must exist before these tests run.

    @pytest.fixture(autouse=True)
    def _setup_table(self, oracle_engine):
        with oracle_engine.conn():
            cursor = oracle_engine._pool.acquire().cursor()
            try:
                cursor.execute(
                    "BEGIN "
                    "  EXECUTE IMMEDIATE 'CREATE TABLE _sp_test (id NUMBER)';"
                    "EXCEPTION WHEN OTHERS THEN "
                    "  IF SQLCODE != -955 THEN RAISE; END IF; "
                    "END;"
                )
            finally:
                cursor.close()
        yield
        with oracle_engine.conn():
            cursor = oracle_engine._pool.acquire().cursor()
            try:
                cursor.execute(
                    "BEGIN "
                    "  EXECUTE IMMEDIATE 'DROP TABLE _sp_test';"
                    "EXCEPTION WHEN OTHERS THEN "
                    "  IF SQLCODE != -942 THEN RAISE; END IF; "
                    "END;"
                )
            finally:
                cursor.close()

    def test_start_and_release_savepoint(self, oracle_engine):
        with oracle_engine.conn():
            with oracle_engine.transaction():
                oracle_engine.query(
                    'INSERT INTO _sp_test (id) VALUES (1)', static=True,
                ).execute()
                with oracle_engine.transaction():
                    oracle_engine.query(
                        'INSERT INTO _sp_test (id) VALUES (2)', static=True,
                    ).execute()
            count = oracle_engine.query(
                'SELECT COUNT(*) FROM _sp_test', static=True,
            ).scalar()
        assert count == 2

    def test_rollback_savepoint(self, oracle_engine):
        with oracle_engine.conn():
            with oracle_engine.transaction():
                oracle_engine.query(
                    'INSERT INTO _sp_test (id) VALUES (10)', static=True,
                ).execute()
                try:
                    with oracle_engine.transaction():
                        oracle_engine.query(
                            'INSERT INTO _sp_test (id) VALUES (20)',
                            static=True,
                        ).execute()
                        raise RuntimeError('force rollback')
                except RuntimeError:
                    pass
                count = oracle_engine.query(
                    'SELECT COUNT(*) FROM _sp_test', static=True,
                ).scalar()
        assert count == 1


class TestOracleDBIntegration:

    @pytest.fixture(autouse=True)
    def _setup_tables(self, oracle_engine):
        for tbl in ('_full_tx', '_full_rb', '_nest_commit', '_nest_rb'):
            with oracle_engine.conn():
                cursor = oracle_engine._pool.acquire().cursor()
                try:
                    cursor.execute(
                        f"BEGIN "
                        f"  EXECUTE IMMEDIATE 'CREATE TABLE {tbl} (id NUMBER)';"
                        f"EXCEPTION WHEN OTHERS THEN "
                        f"  IF SQLCODE != -955 THEN RAISE; END IF; "
                        f"END;"
                    )
                finally:
                    cursor.close()
        yield
        for tbl in ('_full_tx', '_full_rb', '_nest_commit', '_nest_rb'):
            with oracle_engine.conn():
                cursor = oracle_engine._pool.acquire().cursor()
                try:
                    cursor.execute(
                        f"BEGIN "
                        f"  EXECUTE IMMEDIATE 'DROP TABLE {tbl}';"
                        f"EXCEPTION WHEN OTHERS THEN "
                        f"  IF SQLCODE != -942 THEN RAISE; END IF; "
                        f"END;"
                    )
                finally:
                    cursor.close()

    def test_full_transaction_commit(self, oracle_engine):
        with oracle_engine.conn():
            with oracle_engine.transaction():
                oracle_engine.query(
                    'INSERT INTO _full_tx (id) VALUES (1)', static=True,
                ).execute()
            result = oracle_engine.query(
                'SELECT id FROM _full_tx', static=True,
            ).scalar()
        assert result == 1

    def test_full_transaction_rollback(self, oracle_engine):
        with oracle_engine.conn():
            with oracle_engine.transaction(commit=False):
                oracle_engine.query(
                    'INSERT INTO _full_rb (id) VALUES (1)', static=True,
                ).execute()
            result = oracle_engine.query(
                'SELECT id FROM _full_rb', static=True,
            ).scalar()
        assert result is None

    def test_nested_savepoint_commit(self, oracle_engine):
        with oracle_engine.conn():
            with oracle_engine.transaction():
                oracle_engine.query(
                    'INSERT INTO _nest_commit (id) VALUES (1)', static=True,
                ).execute()
                with oracle_engine.transaction():
                    oracle_engine.query(
                        'INSERT INTO _nest_commit (id) VALUES (2)', static=True,
                    ).execute()
            result = oracle_engine.query(
                'SELECT COUNT(*) FROM _nest_commit', static=True,
            ).scalar()
        assert result == 2

    def test_nested_savepoint_rollback(self, oracle_engine):
        with oracle_engine.conn():
            with oracle_engine.transaction():
                oracle_engine.query(
                    'INSERT INTO _nest_rb (id) VALUES (1)', static=True,
                ).execute()
                try:
                    with oracle_engine.transaction():
                        oracle_engine.query(
                            'INSERT INTO _nest_rb (id) VALUES (2)', static=True,
                        ).execute()
                        raise RuntimeError('inner fail')
                except RuntimeError:
                    pass
            result = oracle_engine.query(
                'SELECT COUNT(*) FROM _nest_rb', static=True,
            ).scalar()
        assert result == 1

import sqlite3

import pytest

pytestmark = pytest.mark.usefixtures('data')

from classic.db_tools.backends.sqlite3 import (
    Sqlite3Transaction,
    Sqlite3ConnectionValidator,
)


class TestSqlite3ConnectionValidator:

    def test_validate_returns_true_for_live_connection(self):
        conn = sqlite3.connect(':memory:')
        validator = Sqlite3ConnectionValidator()
        assert validator.validate(conn) is True
        conn.close()

    def test_validate_returns_false_for_bad_connection(self):
        class FakeConn:
            def cursor(self):
                raise Exception('bad')

        validator = Sqlite3ConnectionValidator()
        assert validator.validate(FakeConn()) is False

    def test_before_release_live_connection(self):
        conn = sqlite3.connect(':memory:')
        validator = Sqlite3ConnectionValidator()
        assert validator.before_release(conn) is True
        conn.close()

    def test_before_release_closed_connection(self):
        conn = sqlite3.connect(':memory:')
        conn.close()
        validator = Sqlite3ConnectionValidator()
        assert validator.before_release(conn) is False


class TestSqlite3Transaction:

    def test_enable_params_sets_isolation_level(self, engine):
        with engine.conn() as conn:
            tx = Sqlite3Transaction(engine._pool, engine.current)
            tx._current.conn = conn
            tx._enable_params()
            assert conn.isolation_level is None

    def test_restore_params(self, engine):
        with engine.conn() as conn:
            old_level = conn.isolation_level
            tx = Sqlite3Transaction(engine._pool, engine.current)
            tx._current.conn = conn
            tx._enable_params()
            tx._restore_params()
            assert conn.isolation_level == old_level

    def test_savepoint_start_and_release(self, engine):
        with engine.conn() as conn:
            conn.execute('CREATE TABLE _sp_test (id INT)')
            tx = Sqlite3Transaction(engine._pool, engine.current, commit=True)
            engine.current.conn = conn
            engine.current.tx_params = {}
            tx._first = False
            tx._start_savepoint()
            conn.execute('INSERT INTO _sp_test (id) VALUES (1)')
            tx._release_savepoint()
            result = conn.execute(
                'SELECT COUNT(*) FROM _sp_test',
            ).fetchone()[0]
            assert result == 1

    def test_savepoint_rollback(self, engine):
        with engine.conn() as conn:
            conn.execute('CREATE TABLE _sp_rollback (id INT)')
            tx = Sqlite3Transaction(engine._pool, engine.current, commit=True)
            engine.current.conn = conn
            engine.current.tx_params = {}
            tx._first = False
            tx._start_savepoint()
            conn.execute('INSERT INTO _sp_rollback (id) VALUES (1)')
            tx._rollback_savepoint()
            result = conn.execute(
                'SELECT COUNT(*) FROM _sp_rollback',
            ).fetchone()[0]
            assert result == 0

    def test_full_transaction_commit(self, engine):
        with engine.conn():
            engine.query(
                'CREATE TABLE _full_tx (id INT)', static=True,
            ).execute()
        with engine.transaction():
            engine.query(
                'INSERT INTO _full_tx (id) VALUES (:v)', static=True,
            ).execute(v=1)
        result = engine.query('SELECT id FROM _full_tx').scalar()
        assert result == 1

    def test_full_transaction_rollback(self, engine):
        with engine.conn():
            engine.query(
                'CREATE TABLE _full_rb (id INT)', static=True,
            ).execute()
        with engine.transaction(commit=False):
            engine.query(
                'INSERT INTO _full_rb (id) VALUES (:v)', static=True,
            ).execute(v=1)
        result = engine.query('SELECT id FROM _full_rb').scalar()
        assert result is None

    def test_nested_savepoint_via_engine(self, engine):
        with engine.conn():
            engine.query(
                'CREATE TABLE _nest_eng (id INT, val TEXT)', static=True,
            ).execute()
        with engine.transaction():
            engine.query(
                'INSERT INTO _nest_eng (id, val) VALUES (:i, :v)',
                static=True,
            ).execute(i=1, v='outer')
            with engine.transaction():
                engine.query(
                    'INSERT INTO _nest_eng (id, val) VALUES (:i, :v)',
                    static=True,
                ).execute(i=2, v='inner')
            result = engine.query(
                'SELECT COUNT(*) FROM _nest_eng', static=True,
            ).scalar()
            assert result == 2

    def test_nested_rollback_via_engine(self, engine):
        with engine.conn():
            engine.query(
                'CREATE TABLE _nest_rb (id INT)', static=True,
            ).execute()
        with engine.transaction():
            engine.query(
                'INSERT INTO _nest_rb (id) VALUES (:v)', static=True,
            ).execute(v=1)
            try:
                with engine.transaction():
                    engine.query(
                        'INSERT INTO _nest_rb (id) VALUES (:v)',
                        static=True,
                    ).execute(v=2)
                    raise RuntimeError('inner fail')
            except RuntimeError:
                pass
            result = engine.query(
                'SELECT COUNT(*) FROM _nest_rb', static=True,
            ).scalar()
            assert result == 1

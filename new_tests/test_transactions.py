_TABLE = 'CREATE TEMP TABLE IF NOT EXISTS _titems (id SERIAL PRIMARY KEY, title TEXT, value INT)'
_CLEAR = 'DELETE FROM _titems'
_INSERT = 'INSERT INTO _titems (title, value) VALUES (%(t)s, %(v)s)'
_SELECT = 'SELECT title FROM _titems WHERE title = %(t)s'


def _make_table(conn):
    from psycopg.pq import TransactionStatus
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    conn.autocommit = True
    conn.execute(_TABLE)
    conn.execute(_CLEAR)
    conn.autocommit = False


class TestTransaction:

    def test_commit_on_exit(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            with engine.transaction():
                engine.query(_INSERT, static=True).execute(t='commit_test', v=1)
            with engine.transaction():
                row = engine.query(_SELECT, static=True).one(t='commit_test')
            assert row is not None

    def test_rollback_on_exception(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            try:
                with engine.transaction():
                    engine.query(_INSERT, static=True).execute(t='rollback_test', v=2)
                    raise RuntimeError('boom')
            except RuntimeError:
                pass
            row = engine.query(_SELECT, static=True).one(t='rollback_test')
            assert row is None

    def test_commit_false_rolls_back(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            with engine.transaction(commit=False):
                engine.query(_INSERT, static=True).execute(t='no_commit', v=3)
            row = engine.query(_SELECT, static=True).one(t='no_commit')
            assert row is None

    def test_nested_transaction(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            with engine.transaction():
                engine.query(_INSERT, static=True).execute(t='outer', v=1)
                try:
                    with engine.transaction():
                        engine.query(_INSERT, static=True).execute(t='inner_rollback', v=2)
                        raise RuntimeError('inner fail')
                except RuntimeError:
                    pass
                row = engine.query(_SELECT, static=True).one(t='inner_rollback')
                assert row is None
            with engine.transaction():
                row = engine.query(_SELECT, static=True).one(t='outer')
            assert row is None

    def test_nested_transaction_all_commit(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            with engine.transaction():
                engine.query(_INSERT, static=True).execute(t='nested_outer', v=1)
                with engine.transaction():
                    engine.query(_INSERT, static=True).execute(t='nested_inner', v=2)
            assert engine.query(_SELECT, static=True).one(t='nested_outer') is not None
            assert engine.query(_SELECT, static=True).one(t='nested_inner') is not None

    def test_transaction_params_readonly(self, engine):
        with engine.transaction(readonly=True):
            result = engine.query('SELECT 1 AS a', static=True).scalar()
            assert result == 1

    def test_nested_deferrable(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            with engine.transaction(deferrable=True):
                engine.query(_INSERT, static=True).execute(t='def_outer', v=1)
                with engine.transaction(deferrable=True):
                    engine.query(_INSERT, static=True).execute(t='def_inner', v=2)
            assert engine.query(_SELECT, static=True).one(t='def_outer') is not None
            assert engine.query(_SELECT, static=True).one(t='def_inner') is not None

    def test_transaction_as_decorator(self, engine):
        call_count = 0

        @engine.transaction
        def do_work():
            nonlocal call_count
            call_count += 1
            engine.query('SELECT 1', static=True).execute()

        do_work()
        assert call_count == 1


class TestConn:

    def test_conn_context_manager(self, engine):
        with engine.conn():
            result = engine.query('SELECT 2 AS b', static=True).scalar()
            assert result == 2

    def test_conn_as_decorator(self, engine):
        call_count = 0

        @engine.conn
        def do_work():
            nonlocal call_count
            call_count += 1
            engine.query('SELECT 3', static=True).execute()

        do_work()
        assert call_count == 1

    def test_conn_does_not_commit(self, engine):
        with engine.conn() as conn:
            _make_table(conn)
            engine.query(_INSERT, static=True).execute(t='conn_no_commit', v=99)
        with engine.transaction():
            row = engine.query(_SELECT, static=True).one(t='conn_no_commit')
        assert row is None
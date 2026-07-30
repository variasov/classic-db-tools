import logging
import types

import pytest
import psycopg

from classic.db_tools import Engine

from conftest import connect


class TestEngineConstructor:

    def test_default_uses_driver_connect(self, standalone_engine):
        assert standalone_engine._factory is not None
        assert standalone_engine._pool is not None

    def test_rejects_driver_without_connect(self):
        fake = types.ModuleType('fake')
        with pytest.raises(AssertionError):
            Engine(fake)

    def test_rejects_non_callable_factory(self):
        with pytest.raises(AssertionError):
            Engine(psycopg, 'not_callable')

    def test_custom_factory(self):
        calls = []

        def factory():
            calls.append(1)
            return connect()

        eng = Engine(psycopg, factory)
        conn = eng._pool.acquire()
        eng._pool.release(conn)
        assert len(calls) == 1

    def test_pool_kwargs(self):
        eng = Engine(psycopg, connect, pool_kwargs={'limit': 5, 'timeout': 3.0})
        assert eng._pool.limit == 5
        assert eng._pool.timeout == 3.0

    def test_custom_logger(self):
        logger = logging.getLogger('test_logger')
        eng = Engine(psycopg, connect, logger=logger)
        assert eng._logger is logger

    def test_identifier_quote_char(self):
        eng = Engine(psycopg, connect, identifier_quote_char='`')
        assert eng.dynamic_templates.identifier_quote_char == '`'

    def test_templates_dirs_string(self):
        eng = Engine(psycopg, connect, templates_dirs='/tmp')
        assert eng.templates_paths == ['/tmp']

    def test_templates_dirs_sequence(self):
        eng = Engine(psycopg, connect, templates_dirs=['/a', '/b'])
        assert eng.templates_paths == ['/a', '/b']


class TestEngineQuery:

    def test_static_query(self, engine):
        result = engine.query('SELECT 1 AS num', static=True).scalar()
        assert result == 1

    def test_dynamic_query(self, engine):
        result = engine.query(
            'SELECT {{ x }} AS num', static=False,
        ).scalar(x=42)
        assert result == 42

    def test_static_by_default(self, standalone_engine):
        eng = Engine(
            psycopg, connect, str_templates_static_by_default=True,
        )
        with eng.transaction():
            result = eng.query('SELECT 1').scalar()
        assert result == 1

    def test_dynamic_query_uses_jinja(self, engine):
        result = engine.query(
            'SELECT {{ val }} AS out', static=False,
        ).scalar(val='hello')
        assert result == 'hello'


class TestEngineQueryFrom:

    def test_static_from_file(self, engine):
        engine.query_from('insert_item.sql').execute(t='qff1', v=1)
        row = engine.query_from('get_by_id.sql').one(id=1)
        assert row is not None

    def test_dynamic_from_file(self, engine):
        engine.query_from('insert_item.sql').execute(t='qff2', v=10)
        engine.query_from('insert_item.sql').execute(t='qff3', v=30)
        rows = engine.query_from('filter_by_value.sql.tmpl').all(
            min_value=15,
        )
        assert len(rows) == 1
        assert rows[0][2] == 30

    def test_raises_on_unknown_extension(self, engine):
        with pytest.raises(ValueError, match='Unsupported filename extension'):
            engine.query_from('data.txt')

    def test_raises_on_missing_file(self, engine):
        q = engine.query_from('nonexistent.sql')
        with pytest.raises(FileNotFoundError):
            q._tmpl_factory()


class TestEngineConn:

    def test_context_manager(self, engine):
        with engine.conn():
            result = engine.query(
                'SELECT 10 AS val', static=True,
            ).scalar()
        assert result == 10

    def test_multiple_queries_share_connection(self, engine):
        with engine.conn():
            engine.query_from('insert_item.sql').execute(t='cm1', v=1)
            row = engine.query_from('select_item_by_title.sql').one(t='cm1')
            assert row is not None

    def test_as_decorator(self, engine):
        call_count = 0

        @engine.conn
        def work():
            nonlocal call_count
            call_count += 1
            engine.query('SELECT 1', static=True).execute()

        work()
        assert call_count == 1


class TestEngineTransaction:

    def test_commits_on_success(self, standalone_engine):
        with standalone_engine.transaction():
            standalone_engine.query(
                'CREATE TEMP TABLE IF NOT EXISTS _tx_comm '
                '(id INT)', static=True,
            ).execute()
            standalone_engine.query(
                'INSERT INTO _tx_comm (id) VALUES (%(v)s)', static=True,
            ).execute(v=1)
        with standalone_engine.transaction():
            result = standalone_engine.query(
                'SELECT id FROM _tx_comm', static=True,
            ).scalar()
            assert result == 1

    def test_rollback_on_exception(self, standalone_engine):
        with standalone_engine.transaction():
            standalone_engine.query(
                'CREATE TEMP TABLE IF NOT EXISTS _tx_rb '
                '(id INT)', static=True,
            ).execute()
        try:
            with standalone_engine.transaction():
                standalone_engine.query(
                    'INSERT INTO _tx_rb (id) VALUES (%(v)s)', static=True,
                ).execute(v=2)
                raise RuntimeError('boom')
        except RuntimeError:
            pass
        with standalone_engine.transaction():
            result = standalone_engine.query(
                'SELECT id FROM _tx_rb', static=True,
            ).scalar()
            assert result is None

    def test_commit_false_rolls_back(self, standalone_engine):
        with standalone_engine.transaction():
            standalone_engine.query(
                'CREATE TEMP TABLE IF NOT EXISTS _tx_nc '
                '(id INT)', static=True,
            ).execute()
        with standalone_engine.transaction(commit=False):
            standalone_engine.query(
                'INSERT INTO _tx_nc (id) VALUES (%(v)s)', static=True,
            ).execute(v=3)
        with standalone_engine.transaction():
            result = standalone_engine.query(
                'SELECT id FROM _tx_nc', static=True,
            ).scalar()
            assert result is None

    def test_as_decorator(self, standalone_engine):
        call_count = 0

        @standalone_engine.transaction
        def work():
            nonlocal call_count
            call_count += 1
            standalone_engine.query('SELECT 1', static=True).execute()

        work()
        assert call_count == 1

    def test_nested_transactions_create_savepoint(self, standalone_engine):
        with standalone_engine.transaction():
            standalone_engine.query(
                'CREATE TEMP TABLE IF NOT EXISTS _tx_nest '
                '(id INT, val TEXT)', static=True,
            ).execute()
            standalone_engine.query(
                'INSERT INTO _tx_nest (id, val) VALUES (%(i)s, %(v)s)',
                static=True,
            ).execute(i=1, v='outer')
            with standalone_engine.transaction():
                standalone_engine.query(
                    'INSERT INTO _tx_nest (id, val) VALUES (%(i)s, %(v)s)',
                    static=True,
                ).execute(i=2, v='inner')
            result = standalone_engine.query(
                'SELECT COUNT(*) FROM _tx_nest', static=True,
            ).scalar()
            assert result == 2

    def test_nested_rollback_to_savepoint(self, standalone_engine):
        with standalone_engine.transaction():
            standalone_engine.query(
                'CREATE TEMP TABLE IF NOT EXISTS _tx_nrb '
                '(id INT)', static=True,
            ).execute()
            standalone_engine.query(
                'INSERT INTO _tx_nrb (id) VALUES (%(v)s)', static=True,
            ).execute(v=1)
            try:
                with standalone_engine.transaction():
                    standalone_engine.query(
                        'INSERT INTO _tx_nrb (id) VALUES (%(v)s)', static=True,
                    ).execute(v=2)
                    raise RuntimeError('inner fail')
            except RuntimeError:
                pass
            result = standalone_engine.query(
                'SELECT COUNT(*) FROM _tx_nrb', static=True,
            ).scalar()
            assert result == 1

    def test_transaction_params_readonly(self, standalone_engine):
        with standalone_engine.transaction(readonly=True):
            result = standalone_engine.query(
                'SELECT 1 AS a', static=True,
            ).scalar()
            assert result == 1

    def test_transaction_params_isolation_level(self, standalone_engine):
        with standalone_engine.transaction(level='serializable'):
            result = standalone_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_transaction_params_deferrable(self, standalone_engine):
        with standalone_engine.transaction(
            readonly=True, deferrable=True,
        ):
            result = standalone_engine.query('SELECT 1', static=True).scalar()
            assert result == 1

    def test_nested_transaction_mismatched_params(self, standalone_engine):
        with standalone_engine.transaction(readonly=True):
            with pytest.raises(AssertionError):
                with standalone_engine.transaction():
                    standalone_engine.query(
                        'SELECT 1', static=True,
                    ).execute()

    def test_nested_decorator(self, standalone_engine):

        @standalone_engine.transaction
        def outer():
            standalone_engine.query(
                'CREATE TEMP TABLE IF NOT EXISTS _tx_dec '
                '(id INT)', static=True,
            ).execute()
            standalone_engine.query(
                'INSERT INTO _tx_dec (id) VALUES (%(v)s)', static=True,
            ).execute(v=10)

            @standalone_engine.transaction
            def inner():
                standalone_engine.query(
                    'INSERT INTO _tx_dec (id) VALUES (%(v)s)', static=True,
                ).execute(v=20)

            inner()

        outer()
        with standalone_engine.transaction():
            result = standalone_engine.query(
                'SELECT COUNT(*) FROM _tx_dec', static=True,
            ).scalar()
            assert result == 2


class TestEngineCache:

    def test_string_cache_reuses_template(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 1', static=True)._tmpl_factory()
        assert t1 is t2

    def test_file_cache_reuses_template(self, engine):
        t1 = engine.query_from('get_all.sql')._tmpl_factory()
        t2 = engine.query_from('get_all.sql')._tmpl_factory()
        assert t1 is t2

    def test_different_content_different_instances(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 2', static=True)._tmpl_factory()
        assert t1 is not t2


class TestEngineFilters:

    def test_bind_filter(self, engine):
        result = engine.query(
            'SELECT {{ x }} AS out', static=False,
        ).scalar(x=99)
        assert result == 99

    def test_sqlsafe_filter(self, engine):
        result = engine.query(
            'SELECT {{ expr|sqlsafe }} AS out', static=False,
        ).scalar(expr='1 + 2')
        assert result == 3

    def test_inclause_filter(self, engine):
        result = engine.query(
            'SELECT * FROM (VALUES (1),(2),(3)) AS t(v) '
            'WHERE v IN {{ vals|inclause }}',
            static=False,
        ).all(vals=[1, 3])
        assert len(result) == 2

    def test_identifier_filter(self, engine):
        result = engine.query(
            'SELECT 1 AS {{ col|identifier }}', static=False,
        ).scalar(col='my_col')
        assert result == 1

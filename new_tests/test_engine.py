import pytest
import psycopg


class TestEngineQueryFrom:

    def test_static_query_from_file(self, engine):
        with engine.transaction():
            engine.query(
                'DROP TABLE IF EXISTS test_items CASCADE; '
                'CREATE TEMP TABLE test_items (id SERIAL PRIMARY KEY, title TEXT, value INT)',
                static=True,
            ).execute()
            engine.query_from('save_item.sql').execute(title='foo', value=42)

    def test_dynamic_query_from_file(self, engine):
        with engine.transaction():
            engine.query(
                'DROP TABLE IF EXISTS test_items CASCADE; '
                'CREATE TEMP TABLE test_items (id SERIAL PRIMARY KEY, title TEXT, value INT)',
                static=True,
            ).execute()
            engine.query_from('save_item.sql').execute(title='bar', value=10)
            engine.query_from('save_item.sql').execute(title='baz', value=20)
            rows = engine.query_from('filter_by_value.sql.tmpl').all(min_value=15)
            assert len(rows) == 1
            assert rows[0][1] == 'baz'

    def test_query_from_raises_on_unknown_extension(self, engine):
        with pytest.raises(ValueError, match='Unsupported filename extension'):
            engine.query_from('data.txt')

    def test_query_from_raises_on_missing_file(self, engine):
        q = engine.query_from('nonexistent.sql')
        with pytest.raises(FileNotFoundError):
            q._tmpl_factory()


class TestEngineQuery:

    def test_static_query(self, engine):
        with engine.transaction():
            engine.query('SELECT 1 AS num', static=True).execute()

    def test_dynamic_query(self, engine):
        with engine.transaction():
            result = engine.query('SELECT {{ x }} AS num', static=False).scalar(x=99)
            assert result == 99

    def test_static_by_default_false(self, engine):
        with engine.transaction():
            result = engine.query('SELECT {{ val }} AS out', static=False).scalar(val='hello')
            assert result == 'hello'

    def test_identifier_filter(self, engine):
        with engine.transaction():
            result = engine.query('SELECT 1 AS {{ col|identifier }}', static=False).scalar(col='my_col')
            assert result == 1

    def test_inclause_filter(self, engine):
        with engine.transaction():
            result = engine.query(
                'SELECT * FROM (VALUES (1),(2),(3)) AS t(v) WHERE v IN {{ vals|inclause }}',
                static=False,
            ).all(vals=[1, 3])
            assert len(result) == 2

    def test_sqlsafe_filter(self, engine):
        with engine.transaction():
            result = engine.query('SELECT {{ expr|sqlsafe }} AS out', static=False).scalar(expr='1 + 2')
            assert result == 3

    def test_static_true_does_not_render_jinja(self, engine):
        with engine.transaction():
            with pytest.raises(Exception):
                engine.query('SELECT {{ x|bind }}', static=True).execute(x=1)


class TestEngineCache:

    def test_string_cache_reuses_template(self, engine):
        with engine.transaction():
            t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
            t2 = engine.query('SELECT 1', static=True)._tmpl_factory()
            assert t1 is t2

    def test_file_cache_reuses_template(self, engine):
        with engine.transaction():
            engine.query('CREATE TEMP TABLE IF NOT EXISTS _t2 (id INT)').execute()
            t1 = engine.query_from('get_all.sql')._tmpl_factory()
            t2 = engine.query_from('get_all.sql')._tmpl_factory()
            assert t1 is t2


class TestEngineConstructor:

    def test_without_factory_uses_driver_connect(self):
        from classic.db_tools import Engine
        e = Engine(psycopg)
        assert e._factory is not None

    def test_rejects_driver_without_connect(self):
        from classic.db_tools import Engine
        import types
        fake = types.ModuleType('fake')
        with pytest.raises(AssertionError):
            Engine(fake)

    def test_rejects_non_callable_factory(self):
        from classic.db_tools import Engine
        with pytest.raises(AssertionError):
            Engine(psycopg, 'not callable')

    def test_with_pool_kwargs(self):
        from classic.db_tools import Engine
        eng = Engine(psycopg, pool_kwargs={'limit': 3, 'timeout': 1.5})
        assert eng._pool.limit == 3
        assert eng._pool.timeout == 1.5

    def test_with_custom_logger(self):
        import logging
        from classic.db_tools import Engine
        logger = logging.getLogger('my-test-logger')
        eng = Engine(psycopg, logger=logger)
        assert eng._logger is logger

    def test_with_identifier_quote_char(self):
        from classic.db_tools import Engine
        eng = Engine(psycopg, identifier_quote_char='`')
        assert eng.dynamic_templates.identifier_quote_char == '`'
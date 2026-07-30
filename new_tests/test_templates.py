import pytest


class TestStaticTemplates:

    def test_load_from_file(self, engine):
        with engine.transaction():
            result = engine.query_from('get_all.sql').execute()
            assert result is not None

    def test_load_from_string(self, engine):
        with engine.transaction():
            result = engine.query('SELECT 1 AS x', static=True).scalar()
            assert result == 1

    def test_missing_file(self, engine):
        with pytest.raises(FileNotFoundError):
            q = engine.query_from('does_not_exist.sql')
            q._tmpl_factory()


class TestDynamicTemplates:

    def test_load_from_file(self, engine):
        with engine.transaction():
            result = engine.query(
                'SELECT {{ x }} AS out', static=False,
            ).scalar(x=123)
            assert result == 123

    def test_load_from_string(self, engine):
        with engine.transaction():
            result = engine.query(
                'SELECT {{ x }} AS out', static=False,
            ).scalar(x=123)
            assert result == 123

    def test_template_syntax_error(self, engine):
        with pytest.raises(Exception):
            with engine.transaction():
                engine.query(
                    'SELECT {{ invalid | unknown_filter }} AS out',
                    static=False,
                ).execute()


class TestTemplateCache:

    def test_cache_returns_same_instance(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 1', static=True)._tmpl_factory()
        assert t1 is t2

    def test_different_strings_different_cache_keys(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 2', static=True)._tmpl_factory()
        assert t1 is not t2
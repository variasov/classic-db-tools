import pytest

pytestmark = pytest.mark.usefixtures('data')


class TestStaticTemplates:

    def test_load_from_file(self, engine):
        result = engine.query_from('get_all.sql').execute()
        assert result is not None

    def test_load_from_string(self, engine):
        result = engine.query('SELECT 1 AS val', static=True).scalar()
        assert result == 1

    def test_missing_file_raises(self, engine):
        q = engine.query_from('does_not_exist.sql')
        with pytest.raises(FileNotFoundError):
            q._tmpl_factory()

    def test_file_and_string_different_instances(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query_from('get_all.sql')._tmpl_factory()
        assert t1 is not t2


class TestDynamicTemplates:

    def test_render_simple_variable(self, engine):
        result = engine.query(
            'SELECT {{ val }} AS out', static=False,
        ).scalar(val=99)
        assert result == 99

    def test_render_sql_filter(self, engine):
        result = engine.query(
            'SELECT {{ val|sqlsafe }} AS out', static=False,
        ).scalar(val='1 + 2')
        assert result == 3

    def test_template_syntax_error(self, engine):
        with pytest.raises(Exception):
            engine.query(
                'SELECT {{ invalid | unknown }} AS out',
                static=False,
            ).execute()


class TestTemplateCache:

    def test_string_cache_same_instance(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 1', static=True)._tmpl_factory()
        assert t1 is t2

    def test_different_strings_different_instances(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 2', static=True)._tmpl_factory()
        assert t1 is not t2

    def test_file_cache_same_instance(self, engine):
        t1 = engine.query_from('get_all.sql')._tmpl_factory()
        t2 = engine.query_from('get_all.sql')._tmpl_factory()
        assert t1 is t2

    def test_static_and_dynamic_different(self, engine):
        t1 = engine.query('SELECT 1', static=True)._tmpl_factory()
        t2 = engine.query('SELECT 1', static=False)._tmpl_factory()
        assert t1 is not t2

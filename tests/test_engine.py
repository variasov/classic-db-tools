from classic.db_tools import Engine


def test_from_file(engine: Engine):
    result = engine.from_file('test_render.sql').execute().scalar()

    assert result == 'rendered'


def test_from_str(engine: Engine):
    result = engine.from_str("SELECT 'rendered'").execute().scalar()

    assert result == 'rendered'


def test_queries_collection(engine: Engine):
    result = engine.queries.test_render().scalar()

    assert result == 'rendered'

from unittest.mock import Mock

from classic.db_tools import Engine


def test_queries_cache(engine: Engine):
    for query in (
        engine.from_file('test_render.sql'),
        engine.from_file('test_render.sql.tmpl'),
        engine.from_str('SELECT 1'),
        engine.from_str('SELECT 1', static=True),
    ):
        factory = Mock()
        query.query_factory = factory
        query.execute()
        query.execute()
        factory.assert_called_once()

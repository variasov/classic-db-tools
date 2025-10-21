from unittest.mock import Mock, call, MagicMock

from classic.db_tools import Engine
from classic.db_tools.types import Cursor


def test_queries_cache(engine: Engine):
    for query in (
        engine.query_from('test_render.sql'),
        engine.query_from('test_render.sql.tmpl'),
        engine.query('SELECT 1'),
        engine.query('SELECT 1', static=True),
    ):
        execute = Mock()
        lazy_query = MagicMock()
        lazy_query.__call__ = Mock(return_value=execute)
        cursor = Mock(Cursor)

        query._lazy_query = lazy_query
        query.execute({}, cursor=cursor)
        query.execute({}, cursor=cursor)

        lazy_query.assert_has_calls([
            call(),
            call().execute({}, cursor),
            call(),
            call().execute({}, cursor),
        ])

from unittest.mock import Mock

from classic.db_tools import Engine

from .dto import Task


def fake():
    while True:
        yield


def test_queries_cache(engine: Engine):
    fake_compile = Mock(return_value=fake)
    query = engine.query('SELECT 1 WHERE FALSE').return_as(Task)
    query._compile_mapper = fake_compile

    fake_compile.assert_not_called()

    query.one()
    query.one()

    fake_compile.assert_called_once()

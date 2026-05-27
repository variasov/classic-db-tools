from unittest.mock import Mock

from classic.db_tools import Engine
from classic.db_tools.mapping.mapper import Mapper

from .dto import Task


def fake(rows):
    while True:
        yield


def test_queries_cache(engine: Engine):
    fake_compile = Mock(return_value=fake)
    fake_mapper = Mock(Mapper)
    fake_mapper._compile_mapper_func = fake_compile

    query = engine.query('SELECT 1 WHERE FALSE').map_to(Task)
    query._mapper = fake_mapper

    fake_compile.assert_not_called()

    query.one()
    query.one()

    fake_compile.assert_called_once()

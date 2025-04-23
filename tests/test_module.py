from classic.sql_tools.module import Module
from psycopg import Connection


def test_from_file(queries: Module, connection: Connection):
    q = queries.from_file('test_render.sql')
    result = q.execute(connection).scalar()

    assert result == 'rendered'


def test_from_str(queries: Module, connection: Connection):
    q = queries.from_str("SELECT 'rendered'")
    result = q.execute(connection).scalar()

    assert result == 'rendered'

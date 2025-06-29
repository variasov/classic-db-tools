from classic.sql_tools import Engine
from psycopg import Connection


def test_from_file(engine: Engine, connection: Connection):
    q = engine.from_file('test_render.sql')
    result = q.execute(connection).scalar()

    assert result == 'rendered'


def test_from_str(engine: Engine, connection: Connection):
    q = engine.from_str("SELECT 'rendered'")
    result = q.execute(connection).scalar()

    assert result == 'rendered'

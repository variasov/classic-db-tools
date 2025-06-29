from classic.sql_tools import Engine
from psycopg import Connection, Cursor
import pytest


@pytest.fixture(scope='function')
def cursor(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


def test_execute_from_connection(engine: Engine, connection: Connection):
    result = engine.test_render(connection).scalar()

    assert result == 'rendered'


def test_execute_from_cursor(engine: Engine, cursor: Cursor):
    result = engine.test_render(cursor).scalar()

    assert result == 'rendered'

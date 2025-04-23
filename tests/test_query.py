from classic.sql_tools.module import Module
from psycopg import Connection, Cursor
import pytest


@pytest.fixture(scope='function')
def cursor(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


def test_execute_from_connection(queries: Module, connection: Connection):
    q = queries.from_file('test_render.sql')
    result = q.execute(connection).scalar()

    assert result == 'rendered'


def test_execute_from_cursor(queries: Module, cursor: Cursor):
    q = queries.from_file('test_render.sql')
    result = q.execute(cursor).scalar()

    assert result == 'rendered'

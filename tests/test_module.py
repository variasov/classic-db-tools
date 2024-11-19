import os.path

from classic.sql_tools import Module
from psycopg import connect
import pytest


@pytest.fixture(scope='module')
def queries():
    return Module(os.path.join(os.path.dirname(__file__), 'sql'))


@pytest.fixture(scope='module')
def connection():
    conn = connect()
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture(scope='module', autouse=True)
def db(queries, connection):
    q = queries.from_file('tasks/ddl.sql')
    q.execute(connection)

    q = queries.from_file('tasks/save.sql')
    # q.execute(connection, [
    #     {'name': '1'},
    #     {'name': '2'},
    #     {'name': '3'},
    # ])
    q.execute(connection, {'name': '1'})
    q.execute(connection, {'name': '2'})
    q.execute(connection, {'name': '3'})


def test_many(queries, connection):
    q = queries.from_file('tasks/find_by_name.sql')
    assert q.many(connection, name='1') == [(1, '1')]


def test_one(queries, connection):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.one(connection, id=1) == (1, '1')


def test_scalar(queries, connection):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.scalar(connection, id=1) == 1

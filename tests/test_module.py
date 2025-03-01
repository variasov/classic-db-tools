import os.path
from os import getenv

from classic.sql_tools import Module
from psycopg import connect
import pytest


@pytest.fixture(scope='module')
def queries():
    return Module(os.path.join(os.path.dirname(__file__), 'sql'))


@pytest.fixture
def connection():
    db_url = 'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    params = dict(
        db_user=getenv('DATABASE_USER'),
        db_pass=getenv('DATABASE_PASSWORD'),
        db_host=getenv('DATABASE_HOST', 'localhost'),
        db_port=getenv('DATABASE_PORT', 5432),
        db_name=getenv('DATABASE_NAME'),
    )
    conn = connect(conninfo=db_url.format(**params))
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture(autouse=True)
def db(queries, connection):
    q = queries.from_file('tasks/ddl.sql')
    q.execute(connection)


@pytest.fixture
def fill_db(queries, connection):
    values = [1, 2, 3]
    statuses = ['ready', 'active', 'completed']
    for val in values:
        connection.execute(
            """
            INSERT INTO tasks(name) VALUES (%s);
            """, (val,)
        )

    for id, status in enumerate(statuses, start=1):
        connection.execute(
            """
            INSERT INTO task_status(status, task_id) VALUES (%s, %s);
            """, (status, id)
        )


def test_many(queries, connection, fill_db):
    q = queries.from_file('tasks/find_by_name.sql')
    assert q.execute(connection, {'name': '1'}).many() == [(1, '1')]
    assert q.many(connection, name='1') == [(1, '1')]


def test_one(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.execute(connection, {'id': '1'}).one() == (1, '1')
    assert q.one(connection, id=1) == (1, '1')


def test_scalar(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.execute(connection, {'id': '1'}).scalar() == 1
    assert q.scalar(connection, id=1) == 1


def test_one_or_none(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.execute(connection, {'id': '1'}).one_or_none() == (1, '1')
    assert q.one_or_none(connection, id=1) == (1, '1')


def test_one_or_none_empty(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.execute(connection, {'id': '4'}).one_or_none() is None
    assert q.one_or_none(connection, id=4) is None


def test_insert(queries, connection):
    query = (
        """
        SELECT * FROM tasks;
        """
    )
    assert connection.execute(query).fetchall() == []

    q = queries.from_file('tasks/save.sql')
    q.execute(connection, {'name': '1', 'value': 'value_1'})

    result = connection.execute(query).fetchall()
    assert result == [(1, '1', 'value_1')]


def test_insert_many(queries, connection):
    query = (
        """
        SELECT * FROM tasks;
        """
    )
    assert connection.execute(query).fetchall() == []

    q = queries.from_file('tasks/save.sql')
    q.execute(connection, [
        {'name': '1', 'value': 'value_1'},
        {'name': '2', 'value': 'value_2'},
        {'name': '3', 'value': 'value_3'},
    ])

    result = connection.execute(query).fetchall()
    assert result == [(1, '1', 'value_1'), (2, '2', 'value_2'), (3, '3', 'value_3')]


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
    ]
)
def test_get_by_status(queries, connection, fill_db, status, value):
    q = queries.from_file('tasks/joined_get_by_status.sql')

    assert (
        q.execute(connection, {'status': status}).one_or_none()
        == (value, str(value), status)
    )
    assert (
        q.one_or_none(connection, status=status) == (value, str(value), status)
    )


def test_get_by_status_none(queries, connection, fill_db):
    q = queries.from_file('tasks/joined_get_by_status.sql')

    assert (
        q.execute(connection).many() == [
        (1, '1'), (2, '2'), (3, '3')
    ]
    )


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (1, 'active'),
        (1, 'completed'),
        (1, None)
    ]
)
def test_count_by_status(queries, connection, fill_db, status, value):
    q = queries.from_file('tasks/count_by_status.sql')
    if status:
        assert (
            q.execute(connection, {'status': status}).scalar()
            == value
        )
        assert (
            q.scalar(connection, status=status) == value
        )
    else:
        assert (
            q.execute(connection).scalar()
            == value
        )
        assert (
            q.scalar(connection) == value
        )


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
        (1, None),
    ]
)
def test_sum_tasks(queries, connection, fill_db, status, value):
    q = queries.from_file('tasks/sum_tasks.sql')
    if status:
        assert (
            q.execute(connection, {'status': status}).scalar()
            == value
        )
        assert (
            q.scalar(connection, status=status) == value
        )
    else:
        assert (
            q.execute(connection).scalar()
            == value
        )
        assert (
            q.scalar(connection) == value
        )

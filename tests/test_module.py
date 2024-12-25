import os.path
from os import getenv
import re

from classic.sql_tools import Module
from psycopg import connect
import pytest
import jinja2


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
    for val in values:
        connection.execute(
            """
            INSERT INTO tasks(name) VALUES (%s);
            """, (val,)
        )


def test_many(queries, connection, fill_db):
    q = queries.from_file('tasks/find_by_name.sql')
    #assert q.execute_many(connection, [{'name': '1'}]).many() == [(1, '1')]
    assert q.many(connection, name='1') == [(1, '1')]


def test_one(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    #assert q.execute(connection, name=1).one() == (1, '1')
    assert q.one(connection, id=1) == (1, '1')


def test_scalar(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.scalar(connection, id=1) == 1


def test_one_or_none(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.one_or_none(connection, id=1) == (1, '1')


def test_one_or_none_empty(queries, connection, fill_db):
    q = queries.from_file('tasks/get_by_id.sql')
    assert q.one_or_none(connection, id=4) is None


def test_insert(queries, connection):
    query = (
        """
        SELECT * FROM tasks;
        """
    )
    assert connection.execute(query).fetchall() == []

    q = queries.from_file('tasks/save.sql')
    q.execute(connection, {'name': '1'})

    result = connection.execute(query).fetchall()
    assert result == [(1, '1')]


def test_insert_many(queries, connection):
    # TODO: тест не проходит, Jinja2 не воспринимает множественное вставление
    query = (
        """
        SELECT * FROM tasks;
        """
    )
    assert connection.execute(query).fetchall() == []

    q = queries.from_file('tasks/save.sql')
    q.execute_many(connection, [
        {'name': '1'},
        {'name': '2'},
        {'name': '3'},
    ])

    result = connection.execute(query).fetchall()
    assert result == [(1, '1'), (2, '2'), (3, '3')]


def test_alter_insert_many(queries, connection):
    query = (
        """
        SELECT * FROM tasks;
        """
    )
    assert connection.execute(query).fetchall() == []

    q = queries.from_file('tasks/insert_many.sql')
    q.execute(
        connection,
        {'tasks': [1, 2, 3]},
    )

    result = connection.execute(query).fetchall()
    assert result == [(1, '1'), (2, '2'), (3, '3')]


def get_query(params: dict | list) -> str:
    """
    Идея по доработке sql в случае множественной вставки. Минус - мы работаем с
    template, соответственно нет возможности модернизировать query
    """
    environment = jinja2.Environment()
    query = "INSERT INTO tasks(name, value) VALUES ({{ name, value }});"
    if isinstance(params, dict):  # Если передан один объект
        template = environment.from_string(query)
        return template.render(params)
    elif isinstance(params, list):  # Если передан список объектов
        values = []
        keys = None
        for param in params:
            if not keys:
                keys = tuple(param.keys())
                values.append(tuple(param.values()))
                continue

            values.append(tuple(param[key] for key in keys))

        values = ", ".join(f"({', '.join(value)})" for value in values)
        modified_query = re.sub(r'{{.*?}}', '{{values}}', query)
        template = environment.from_string(modified_query)
        return template.render(values=values)
    else:
        raise ValueError("Invalid input")


def test__get_query():
    result_dict = get_query({"name": "test1", "value": "test1"})
    result_list = get_query([{"name": "test1", "value": "test1"}, {"name": "test2", "value": "test2"}])

    assert result_dict == "INSERT INTO tasks(name, value) VALUES (('test1', 'test1'));"
    assert result_list == "INSERT INTO tasks(name, value) VALUES (('test1', 'test1'), ('test2', 'test2'));"

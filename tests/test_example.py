import pytest


@pytest.fixture
def ddl(queries, connection):
    q = queries.from_file('example/ddl.sql')
    q.execute(connection)
    yield


@pytest.fixture
def tasks(queries, connection, ddl):
    values = [1, 2, 3]
    statuses = ['ready', 'active', 'completed']
    for val in values:
        connection.execute('INSERT INTO tasks(name) VALUES (%s);', (val,))

    for id, status in enumerate(statuses, start=1):
        connection.execute(
            'INSERT INTO task_status(status, task_id) VALUES (%s, %s);',
            (status, id)
        )

    yield


def test_execute(queries, connection, tasks):
    q = queries.from_file('example/find_by_name.sql')
    assert q(connection, name='1').many() == [(1, '1')]


def test_one(queries, connection, tasks):
    q = queries.from_file('example/get_by_id.sql')
    assert q(connection, id=1).one() == (1, '1')


def test_scalar(queries, connection, tasks):
    q = queries.from_file('example/get_by_id.sql')
    assert q(connection, id=1).scalar() == 1


def test_one_or_none(queries, connection, tasks):
    q = queries.from_file('example/get_by_id.sql')
    assert q(connection, id='1').one() == (1, '1')


def test_insert(queries, connection, ddl):
    q = queries.from_file('example/count.sql')
    assert q(connection).scalar() == 0

    q = queries.from_file('example/save.sql')
    row_id = q(connection, name='1', value='value_1').scalar()

    q = queries.from_file('example/get_all.sql')
    assert q(connection, id=row_id).one() == (row_id, '1', 'value_1')


def test_insert_many(queries, connection, ddl):
    q = queries.from_file('example/count.sql')
    assert q(connection).scalar() == 0

    q = queries.from_file('example/save.sql')
    q(connection, [
        {'name': '1', 'value': 'value_1'},
        {'name': '2', 'value': 'value_2'},
        {'name': '3', 'value': 'value_3'},
    ])

    q = queries.from_file('example/get_all.sql')
    assert q(connection).many() == [
        (1, '1', 'value_1'),
        (2, '2', 'value_2'),
        (3, '3', 'value_3'),
    ]


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
    ]
)
def test_get_by_status(queries, connection, tasks, status, value):
    q = queries.from_file('example/joined_get_by_status.sql')
    assert q(connection, status=status).one() == (
        value, str(value), status
    )


def test_get_by_status_none(queries, connection, tasks):
    q = queries.from_file('example/joined_get_by_status.sql')
    assert q(connection).many() == [
        (1, '1'), (2, '2'), (3, '3'),
    ]


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (1, 'active'),
        (1, 'completed'),
        (1, None)
    ]
)
def test_count_by_status(queries, connection, tasks, status, value):
    q = queries.from_file('example/count_by_status.sql')
    if status:
        assert q(connection, status=status).scalar() == value
    else:
        assert q(connection).scalar() == value


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
        (1, None),
    ]
)
def test_sum_tasks(queries, connection, tasks, status, value):
    q = queries.from_file('example/sum_tasks.sql')
    if status:
        assert q(connection, status=status).scalar() == value
    else:
        assert q(connection).scalar() == value

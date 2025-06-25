import pytest


@pytest.fixture
def ddl(queries, connection):
    queries.example.ddl(connection)
    yield


@pytest.fixture
def tasks(queries, connection, ddl):
    queries.example.save_task(connection, [
        {'name': '1', 'value': 'value_1'},
        {'name': '2', 'value': 'value_2'},
        {'name': '3', 'value': 'value_3'},
    ])
    queries.example.save_task_statuses(connection, [
        {'status': 'ready', 'task_id': 1},
        {'status': 'active', 'task_id': 2},
        {'status': 'completed', 'task_id': 3},
    ])
    yield


def test_execute(queries, connection, tasks):
    assert queries.example.find_by_name(
        connection, name='1',
    ).many() == [(1, '1')]


def test_one(queries, connection, tasks):
    assert queries.example.get_by_id(connection, id='1').one() == (1, '1')


def test_scalar(queries, connection, tasks):
    assert queries.example.get_by_id(connection, id='1').scalar() == 1


def test_one_or_none(queries, connection, tasks):
    assert queries.example.get_by_id(connection, id='1').one() == (1, '1')


def test_insert(queries, connection, ddl):
    assert queries.example.count(connection).scalar() == 0

    row_id = queries.example.save_task(
        connection, name='1', value='value_1',
    ).scalar()

    assert queries.example.get_all(
        connection, id=row_id,
    ).one() == (row_id, '1', 'value_1')


def test_insert_many(queries, connection, ddl):
    assert queries.example.count(connection).scalar() == 0

    queries.example.save_task(connection, [
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
    assert queries.example.joined_get_by_status(
        connection, status=status,
    ).one() == (value, str(value), status)


def test_get_by_status_none(queries, connection, tasks):
    assert queries.example.joined_get_by_status(connection).many() == [
        (1, '1'), (2, '2'), (3, '3'),
    ]


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (1, 'active'),
        (1, 'completed'),
        (3, None)
    ]
)
def test_count_by_status(queries, connection, tasks, value, status):
    assert queries.example.count_by_status(
        connection, status=status,
    ).scalar() == value


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
    ]
)
def test_sum_tasks(queries, connection, tasks, status, value):
    assert queries.example.sum_tasks(
        connection, status=status,
    ).scalar() == value

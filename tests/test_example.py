import pytest

from classic.db_tools import Engine


@pytest.fixture
def tasks(engine: Engine, ddl):
    engine.queries.example.save_task([
        {'name': '1', 'value': 'value_1'},
        {'name': '2', 'value': 'value_2'},
        {'name': '3', 'value': 'value_3'},
    ])
    engine.queries.example.save_task_statuses([
        {'status': 'ready', 'task_id': 1},
        {'status': 'active', 'task_id': 2},
        {'status': 'completed', 'task_id': 3},
    ])
    yield


def test_execute(engine: Engine, tasks):
    assert engine.queries.example.find_by_name(name='1').many() == [(1, '1')]


def test_one(engine: Engine, tasks):
    assert engine.queries.example.get_by_id(id='1').one() == (1, '1')


def test_scalar(engine: Engine, tasks):
    assert engine.queries.example.get_by_id(id='1').scalar() == 1


def test_one_or_none(engine: Engine, tasks):
    assert engine.queries.example.get_by_id(id='1').one() == (1, '1')


def test_insert(engine: Engine, ddl):
    assert engine.queries.example.count().scalar() == 0

    row_id = engine.queries.example.save_task(
        name='1', value='value_1',
    ).scalar()

    assert engine.queries.example.get_all(
        id=row_id,
    ).one() == (row_id, '1', 'value_1')


def test_insert_many(engine: Engine, ddl):
    assert engine.queries.example.count().scalar() == 0

    engine.queries.example.save_task([
        {'name': '1', 'value': 'value_1'},
        {'name': '2', 'value': 'value_2'},
        {'name': '3', 'value': 'value_3'},
    ])

    assert engine.queries.example.get_all().many() == [
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
def test_get_by_status(engine: Engine, tasks, status, value):
    assert engine.queries.example.joined_get_by_status(
        status=status,
    ).one() == (value, str(value), status)


def test_get_by_status_none(engine: Engine, tasks):
    assert engine.queries.example.joined_get_by_status().many() == [
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
def test_count_by_status(engine: Engine, tasks, value, status):
    assert engine.queries.example.count_by_status(
        status=status,
    ).scalar() == value


@pytest.mark.parametrize(
    'value,status', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
    ]
)
def test_sum_tasks(engine: Engine, tasks, status, value):
    assert engine.queries.example.sum_tasks(
        status=status,
    ).scalar() == value

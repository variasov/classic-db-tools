import pytest


def test_execute(engine, connection, tasks):
    assert engine.example.find_by_name(
        connection, name='First',
    ).many() == [(1, 'First')]


def test_one(engine, connection, tasks):
    assert engine.example.get_by_id(connection, id=1).one() == (1, 'First')


def test_scalar(engine, connection, tasks):
    assert engine.example.get_by_id(connection, id=1).scalar() == 1


def test_one_or_none(engine, connection, tasks):
    assert engine.example.get_by_id(connection, id=1).one() == (1, 'First')


def test_insert(engine, connection, ddl):
    assert engine.example.count(connection).scalar() == 0

    row_id = engine.example.save_task(connection, id=1, name='Some task').scalar()

    assert engine.example.get_all(
        connection, id=row_id,
    ).one() == (row_id, 'Some task')


def test_insert_many(engine, connection, ddl):
    assert engine.example.count(connection).scalar() == 0

    engine.example.save_task(connection, [
        {'id': 1, 'name': 'Task 1'},
        {'id': 2, 'name': 'Task 2'},
        {'id': 3, 'name': 'Task 3'},
    ])

    q = engine.from_file('example/get_all.sql')
    assert q(connection).many() == [
        (1, 'Task 1'),
        (2, 'Task 2'),
        (3, 'Task 3'),
    ]


def test_joined_query(engine, connection, tasks):
    assert engine.example.joined_get_by_name(
        connection, name='First',
    ).many() == [
        (1, 'First', 'CREATED'),
        (1, 'First', 'STARTED'),
        (1, 'First', 'FINISHED'),
    ]


@pytest.mark.parametrize(
    'value,status', [
        (6, 'CREATED'),
        (1, 'STARTED'),
        (1, 'FINISHED'),
    ]
)
def test_sum_tasks(engine, connection, tasks, value, status):
    assert engine.example.sum_tasks(
        connection, status=status,
    ).scalar() == value

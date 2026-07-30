from dataclasses import dataclass

import pytest

from classic.db_tools import Engine


@dataclass
class Task:
    name: str
    value: str


@pytest.fixture
def tasks(engine: Engine, ddl):
    _ = ddl
    engine.query_from('example/save_task.sql').executemany([
        {'name': '1', 'value': 'value_1', 'group': 1},
        {'name': '2', 'value': 'value_2', 'group': 1},
        {'name': '3', 'value': 'value_3', 'group': 2},
    ])
    engine.query_from('example/save_task_statuses.sql').executemany([
        {'title': 'ready', 'task_id': 1},
        {'title': 'active', 'task_id': 2},
        {'title': 'completed', 'task_id': 3},
    ])


def test_iter(engine: Engine, tasks):
    _ = tasks
    result = engine.query_from('example/find_by_name.sql.tmpl').iter(
        dict(name='1'),
    )
    assert next(result) == (1, '1')
    with pytest.raises(StopIteration):
        print(next(result))


def test_all(engine: Engine, tasks):
    _ = tasks
    assert engine.query_from(
        'example/find_by_name.sql.tmpl'
    ).all(name='1') == [(1, '1')]


def test_one(engine: Engine, tasks):
    _ = tasks
    assert engine.query_from(
        'example/get_by_id.sql'
    ).one(id='1') == (1, '1')


def test_scalar(engine: Engine, tasks):
    _ = tasks
    assert engine.query_from(
        'example/get_by_id.sql'
    ).scalar(id='1') == 1


def test_scalars(engine: Engine, tasks):
    _ = tasks
    assert list(
        engine.query_from(
            'example/get_all.sql'
        ).scalars()
    ) == [1, 2, 3]


def test_insert(engine: Engine, ddl):
    _ = ddl
    assert engine.query_from('example/count.sql').scalar() == 0

    row_id = engine.query_from('example/save_task.sql').scalar(
        dict(name='1', value='value_1', group=1),
    )

    assert (
        engine.query_from('example/get_all.sql').one(id=row_id)
        == (row_id, '1', 'value_1')
    )


def test_insert_many(engine: Engine, ddl):
    _ = ddl

    assert engine.query_from('example/count.sql').scalar() == 0

    engine.query_from('example/save_task.sql').executemany([
        {'name': '1', 'value': 'value_1', 'group': 1},
        {'name': '2', 'value': 'value_2', 'group': 2},
        {'name': '3', 'value': 'value_3', 'group': 3},
    ])

    assert engine.query_from('example/get_all.sql').all() == [
        (1, '1', 'value_1'),
        (2, '2', 'value_2'),
        (3, '3', 'value_3'),
    ]


@pytest.mark.parametrize(
    'value,title', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
    ]
)
def test_get_by_status(engine: Engine, tasks, title, value):
    _ = tasks
    assert engine.query_from(
        'example/joined_get_by_status.sql.tmpl'
    ).one(title=title) == (value, str(value), title)


def test_get_by_status_none(engine: Engine, tasks):
    _ = tasks
    assert engine.query_from(
        'example/joined_get_by_status.sql.tmpl').all() == [
        (1, '1'), (2, '2'), (3, '3'),
    ]


@pytest.mark.parametrize(
    'value,title', [
        (1, 'ready'),
        (1, 'active'),
        (1, 'completed'),
        (3, None)
    ]
)
def test_count_by_status(engine: Engine, tasks, value, title):
    _ = tasks
    assert engine.query_from(
        'example/count_by_status.sql.tmpl'
    ).scalar(title=title) == value


@pytest.mark.parametrize(
    'value,title', [
        (1, 'ready'),
        (2, 'active'),
        (3, 'completed'),
    ]
)
def test_sum_tasks(engine: Engine, tasks, title, value):
    _ = tasks
    assert engine.query_from(
        'example/sum_tasks.sql.tmpl'
    ).scalar(title=title) == value

from typing import TypedDict, List

import pytest

from classic.db_tools import Engine, Entity, Append, Value


class Status(TypedDict):
    id: int
    title: str


class Task(TypedDict):
    id: int
    name: str
    statuses: List[Status]


mapper = dict(
    task=Entity(Task, 'id', statuses=Append('Status')),
    status=Value(Status, True),
)


sql = '''
    SELECT
        tasks.id            AS Task__id,
        tasks.name          AS Task__name,
        task_status.id      AS Status__id,
        task_status.title   AS Status__title
    FROM tasks
    LEFT JOIN task_status ON task_status.task_id = tasks.id
    ORDER BY tasks.id, task_status.id 
'''


@pytest.fixture
def tasks(engine: Engine, ddl):
    engine.query_from('example/save_task.sql').executemany([
        {'name': 'First', 'value': '', 'group': 1},
        {'name': 'Second', 'value': '', 'group': 1},
        {'name': 'Third', 'value': '', 'group': 2},
        {'name': 'Four', 'value': '', 'group': 2},
    ])
    engine.query_from('example/save_task_statuses.sql').executemany([
        {'title': 'CREATED', 'task_id': 1},
        {'title': 'CREATED', 'task_id': 2},
        {'title': 'CREATED', 'task_id': 3},
        {'title': 'STARTED', 'task_id': 1},
        {'title': 'FINISHED', 'task_id': 1},
    ])
    yield


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__all(engine: Engine, ddl, tasks, static):
    query = engine.query(sql, static=static).map_to(Task, **mapper)
    assert query.all() == [
        Task(id=1, name='First', statuses=[
            Status(id=1, title='CREATED'),
            Status(id=4, title='STARTED'),
            Status(id=5, title='FINISHED'),
        ]),
        Task(id=2, name='Second', statuses=[
            Status(id=2, title='CREATED'),
        ]),
        Task(id=3, name='Third', statuses=[
            Status(id=3, title='CREATED'),
        ]),
        Task(id=4, name='Four', statuses=[]),
    ]


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__all__empty(engine: Engine, ddl, tasks, static):
    assert engine.query('''
        SELECT
            1 AS Task__id,
            1 AS Task__name,
            1 AS Status__id,
            1 AS Status__title
        FROM tasks
        WHERE FALSE
    ''', static=static).map_to(
        Task, **mapper,
    ).all() == []


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__one(engine: Engine, ddl, tasks, static):
    obj = engine.query(sql, static=static).map_to(Task, **mapper).one()
    assert obj == Task(id=1, name='First', statuses=[
        Status(id=1, title='CREATED'),
        Status(id=4, title='STARTED'),
        Status(id=5, title='FINISHED'),
    ])

from typing import TypedDict

import pytest

from classic.db_tools import Engine, OneToOne, OneToMany


class Status(TypedDict):
    id: int
    title: str


class Task(TypedDict):
    id: int
    name: str
    statuses: list['Status']



sql = '''
    SELECT
        tasks.id            AS Task__id,
        tasks.name          AS Task__name,
        task_status.id      AS Status__id,
        task_status.title   AS Status__title
    FROM tasks
    JOIN task_status ON task_status.task_id = tasks.id
    ORDER BY tasks.id, task_status.id 
'''


@pytest.fixture
def tasks(engine: Engine, ddl):
    engine.query_from('example/save_task.sql').executemany([
        {'name': 'First', 'value': ''},
        {'name': 'Second', 'value': ''},
        {'name': 'Third', 'value': ''},
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
    assert engine.query(sql, static=static).return_as(
        Task,
        OneToMany(Task, 'statuses', Status),
    ).all() == [
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
    ]


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__one(engine: Engine, ddl, tasks, static):
    assert engine.query(sql, static=static).return_as(
        Task,
        OneToMany(Task, 'statuses', Status),
    ).one() == Task(id=1, name='First', statuses=[
        Status(id=1, title='CREATED'),
        Status(id=4, title='STARTED'),
        Status(id=5, title='FINISHED'),
    ])


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_split__all(engine: Engine, ddl, tasks, static):
    assert engine.query(sql, static=static).return_as(
        tuple[Task, Status],
    ).all() == [
        (Task(id=1, name='First'), Status(id=1, title='CREATED')),
        (Task(id=1, name='First'), Status(id=4, title='STARTED')),
        (Task(id=1, name='First'), Status(id=5, title='FINISHED')),
        (Task(id=2, name='Second'), Status(id=2, title='CREATED')),
        (Task(id=3, name='Third'), Status(id=3, title='CREATED')),
    ]


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_split__one(engine: Engine, ddl, tasks, static):
    assert engine.query(
        sql, static=static
    ).return_as(
        tuple[Task, Status],
    ).one() == (Task(id=1, name='First'), Status(id=1, title='CREATED'))

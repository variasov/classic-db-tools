from typing import Annotated

import pytest

from classic.db_tools import Engine, OneToMany, ID, Name

from .dto import Task, Status


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
        (Task(id=1, name='First', statuses=[]), Status(id=1, title='CREATED')),
        (Task(id=1, name='First', statuses=[]), Status(id=4, title='STARTED')),
        (Task(id=1, name='First', statuses=[]), Status(id=5, title='FINISHED')),
        (Task(id=2, name='Second', statuses=[]), Status(id=2, title='CREATED')),
        (Task(id=3, name='Third', statuses=[]), Status(id=3, title='CREATED')),
    ]


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_split__one(engine: Engine, ddl, tasks, static):
    assert engine.query(
        sql, static=static
    ).return_as(
        tuple[Task, Status],
    ).one() == (
        Task(id=1, name='First', statuses=[]),
        Status(id=1, title='CREATED'),
    )


def test_custom_name(engine: Engine):
    assert engine.query('''
    SELECT 
        data.task_id        AS custom__id,
        data.task_name      AS custom__name,
        data.status_id      AS another__id,
        data.status_title   AS another__title
    FROM (
        VALUES
            (1, 'First', 1, 'CREATED'),
            (1, 'First', 4, 'STARTED'),
            (1, 'First', 5, 'FINISHED')
    ) AS data(task_id, task_name, status_id, status_title)
    ''').return_as(
        Annotated[Task, Name('custom')],
        OneToMany('custom', 'statuses', Annotated[Status, ID('id'), Name('another')]),
    ).one() == Task(id=1, name='First', statuses=[
        Status(id=1, title='CREATED'),
        Status(id=4, title='STARTED'),
        Status(id=5, title='FINISHED'),
    ])

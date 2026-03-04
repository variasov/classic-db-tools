from dataclasses import dataclass

import pytest

from classic.db_tools import Engine, Entity

from .dto import Task, Status


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


mapping = {
    Task: Entity(id='id'),
    Status: Entity(id='id'),
}


@pytest.fixture
def tasks(engine: Engine, ddl):
    engine.query_from('example/save_task.sql').executemany([
        {'name': 'First', 'value': ''},
        {'name': 'Second', 'value': ''},
        {'name': 'Third', 'value': ''},
        {'name': 'Four', 'value': ''},
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
    assert engine.query(sql, static=static).map_to(Task, mapping).all() == [
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
def test_returning_with_rels__one(engine: Engine, ddl, tasks, static):
    assert engine.query(sql, static=static).map_to(Task, mapping).one() == (
        Task(id=1, name='First', statuses=[
            Status(id=1, title='CREATED'),
            Status(id=4, title='STARTED'),
            Status(id=5, title='FINISHED'),
        ])
    )


# @pytest.mark.parametrize('static', (True, False))
# def test_returning_with_split__all(engine: Engine, ddl, tasks, static):
#     query = engine.query(sql, static=static).return_as(
#         tuple[Task, Status],
#     )
#     assert query.all() == [
#         (Task(id=1, name='First', statuses=[]), Status(id=1, title='CREATED')),
#         (Task(id=1, name='First', statuses=[]), Status(id=4, title='STARTED')),
#         (Task(id=1, name='First', statuses=[]), Status(id=5, title='FINISHED')),
#         (Task(id=2, name='Second', statuses=[]), Status(id=2, title='CREATED')),
#         (Task(id=3, name='Third', statuses=[]), Status(id=3, title='CREATED')),
#         (Task(id=4, name='Four', statuses=[]), Status(id=None, title=None)),
#     ]


# @pytest.mark.parametrize('static', (True, False))
# def test_returning_with_split__one(engine: Engine, ddl, tasks, static):
#     assert engine.query(
#         sql, static=static
#     ).return_as(
#         tuple[Task, Status],
#     ).one() == (
#         Task(id=1, name='First', statuses=[]),
#         Status(id=1, title='CREATED'),
#     )


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
    ''').map_to(
        Task,
        {
            Task: Entity(id='id', prefix='custom'),
            Status: Entity(id='id', prefix='another'),
        },
    ).one() == Task(id=1, name='First', statuses=[
        Status(id=1, title='CREATED'),
        Status(id=4, title='STARTED'),
        Status(id=5, title='FINISHED'),
    ])


@dataclass
class SomeObj:
    id: int
    value: str


@dataclass
class AnotherObj:
    id: int
    some_obj: SomeObj = None


def test_one_to_one(engine: Engine):
    assert engine.query('''
    SELECT 
        data.AnotherObj__id as AnotherObj__id,
        data.SomeObj__id as SomeObj__id,
        data.SomeObj__value as SomeObj__value
    FROM (
        VALUES
            (1, 1, 'VALUE'),
            (2, 1, 'VALUE')
    ) AS data(AnotherObj__id, SomeObj__id, SomeObj__value)
    ''').map_to(
        AnotherObj,
        {
            AnotherObj: Entity(id='id'),
            SomeObj: Entity(id='id'),
        }
    ).all() == [
        AnotherObj(id=1, some_obj=SomeObj(1, 'VALUE')),
        AnotherObj(id=2, some_obj=SomeObj(1, 'VALUE')),
    ]

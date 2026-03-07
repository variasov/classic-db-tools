from dataclasses import dataclass

import pytest

from classic.db_tools import Engine, Mapper, Entity, Append, Value

from .dto import Task, Status


sql = '''
    SELECT
        tasks.id            AS task__id,
        tasks.name          AS task__name,
        task_status.id      AS status__id,
        task_status.title   AS status__title
    FROM tasks
    LEFT JOIN task_status ON task_status.task_id = tasks.id
    ORDER BY tasks.id, task_status.id 
'''


mapper = Mapper(
    task=Entity(Task, 'id', statuses=Append('status')),
    status=Value(Status),
)


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
    objects = engine.query(sql, static=static).map_to(Task, mapper=mapper).all()
    assert objects == [
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
    obj = engine.query(sql, static=static).map_to(Task, mapper=mapper).one()
    assert obj == (
        Task(id=1, name='First', statuses=[
            Status(id=1, title='CREATED'),
            Status(id=4, title='STARTED'),
            Status(id=5, title='FINISHED'),
        ])
    )


def test_custom_name(engine: Engine):
    objects = engine.query('''
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
        prefix='custom',
        mapper=Mapper(
            custom=Entity(Task, 'id', statuses=Append('another')),
            another=Entity(Status, 'id'),
        ),
    ).one()
    assert objects == Task(id=1, name='First', statuses=[
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
    objects = engine.query('''
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
        mapper=Mapper(
            AnotherObj=Entity(AnotherObj, 'id'),
            SomeObj=Entity(SomeObj, 'id'),
        )
    ).all()
    assert objects == [
        AnotherObj(id=1, some_obj=SomeObj(1, 'VALUE')),
        AnotherObj(id=2, some_obj=SomeObj(1, 'VALUE')),
    ]

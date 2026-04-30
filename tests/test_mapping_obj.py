from dataclasses import dataclass

import pytest

from classic.db_tools import Engine, Entity, Append, Value

from .dto import Task, Status, TaskGroup


sql = '''
    SELECT
        tasks.group_        AS task_group__id,
        tasks.id            AS task__id,
        tasks.name          AS task__name,
        task_status.id      AS status__id,
        task_status.title   AS status__title
    FROM tasks
    LEFT JOIN task_status ON task_status.task_id = tasks.id
    ORDER BY tasks.id, task_status.id 
'''


mapper = dict(
    task_group=Entity(TaskGroup, 'id'),
    task=Entity(Task, 'id'),
    status=Value(Status),
)


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
    objects = query.all()
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
def test_returning_with_rels__all__nested(engine: Engine, ddl, tasks, static):
    query = engine.query(sql, static=static).map_to(TaskGroup, 'task_group', **mapper)
    assert query.all() == [
        TaskGroup(id=1, tasks=[
            Task(id=1, name='First', statuses=[
                Status(id=1, title='CREATED'),
                Status(id=4, title='STARTED'),
                Status(id=5, title='FINISHED'),
            ]),
            Task(id=2, name='Second', statuses=[
                Status(id=2, title='CREATED'),
            ]),
        ]),
        TaskGroup(id=2, tasks=[
            Task(id=3, name='Third', statuses=[
                Status(id=3, title='CREATED'),
            ]),
            Task(id=4, name='Four', statuses=[]),
        ]),
    ]


@pytest.mark.parametrize('static', (True, False))
def test_returning_with_rels__one(engine: Engine, ddl, tasks, static):
    obj = engine.query(sql, static=static).map_to(Task, **mapper).one()
    assert obj == (
        Task(id=1, name='First', statuses=[
            Status(id=1, title='CREATED'),
            Status(id=4, title='STARTED'),
            Status(id=5, title='FINISHED'),
        ])
    )


def test_custom_name(engine: Engine):
    query = engine.query('''
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
        'custom',
        custom=Entity(Task, 'id', statuses=Append('another')),
        another=Entity(Status, 'id'),
    )
    assert query.one() == Task(id=1, name='First', statuses=[
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
    query = engine.query('''
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
        AnotherObj=Entity(AnotherObj, 'id'),
        SomeObj=Entity(SomeObj, 'id'),
    )
    assert query.all() == [
        AnotherObj(id=1, some_obj=SomeObj(1, 'VALUE')),
        AnotherObj(id=2, some_obj=SomeObj(1, 'VALUE')),
    ]


def test_value(engine: Engine):
    query = engine.query('''
        SELECT 
            data.task_id        AS custom__id,
            data.task_name      AS custom__name
        FROM (
            VALUES (1, 'First')
        ) AS data(task_id, task_name)
    ''').map_to(
        Task,
        'custom',
        custom=Value(Task),
    )
    assert query.one() == Task(id=1, name='First', statuses=[])


from dataclasses import dataclass
from typing import Annotated

from classic.sql_tools import Module, ToCls, ToDict, OneToMany
from classic.components import factory
from psycopg import Connection


@dataclass
class Task:
    id: Annotated[int, 'id']
    name: str
    statuses: list['TaskStatus'] = factory(list)


@dataclass
class TaskStatus:
    id: Annotated[int, 'id']
    status: str


query = '''
    SELECT
        tasks.id           AS Task__id,
        tasks.name         AS Task__name,
        task_status.id     AS TaskStatus__id,
        task_status.status AS TaskStatus__status
    FROM tasks
    JOIN task_status ON task_status.task_id = tasks.id
'''


def test_returning_with_cls(queries: Module, connection: Connection, ddl, tasks):
    assert queries.from_str(query).execute(
        connection
    ).returning(
        ToCls(Task, id='id'),
        ToCls(TaskStatus, id='id'),
        OneToMany(Task, 'statuses', TaskStatus),
        returns=Task,
    ).many() == [
        Task(id=1, name='First', statuses=[
            TaskStatus(id=1, status='CREATED'),
            TaskStatus(id=4, status='STARTED'),
            TaskStatus(id=5, status='FINISHED'),
        ]),
        Task(id=2, name='Second', statuses=[
            TaskStatus(id=2, status='CREATED'),
        ]),
        Task(id=3, name='Third', statuses=[
            TaskStatus(id=3, status='CREATED'),
        ]),
    ]


def test_returning_with_dict(queries: Module, connection: Connection, ddl, tasks):
    assert queries.from_str(query).execute(
        connection
    ).returning(
        ToDict('Task', id='id'),
        ToDict('TaskStatus', id='id'),
        OneToMany('Task', 'statuses', 'TaskStatus'),
        returns='Task',
    ).many() == [
        dict(id=1, name='First', statuses=[
            dict(id=1, status='CREATED'),
            dict(id=4, status='STARTED'),
            dict(id=5, status='FINISHED'),
        ]),
        dict(id=2, name='Second', statuses=[
            dict(id=2, status='CREATED'),
        ]),
        dict(id=3, name='Third', statuses=[
            dict(id=3, status='CREATED'),
        ]),
    ]

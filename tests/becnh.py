from dataclasses import dataclass
import time
from typing import Annotated
from unittest.mock import Mock

from classic.sql_tools import Engine, ToCls, OneToMany
from classic.components import factory
import psycopg


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


def test_returning_speed(engine: Engine, connection):
    cursor = Mock(psycopg.Cursor)
    cursor.fetchall = Mock(
        return_value=[
            (1, 'First', 1, 'CREATED'),
            (1, 'First', 4, 'STARTED'),
            (1, 'First', 5, 'FINISHED'),
            (2, 'Second', 2, 'CREATED'),
            (3, 'Third', 3, 'CREATED'),
        ] * 1000,
    )
    cursor.description = [
        ('task__id', int),
        ('task__name', str),
        ('taskstatus__id', int),
        ('taskstatus__status', str),
    ]
    cursor.connection = Mock(psycopg.Connection)


    for index in range(3):
        print(f'Attempt {index}')

        started_at = time.time()
        q = engine.from_str(query).execute(
            cursor
        ).returning(
            ToCls(Task, id='id'),
            ToCls(TaskStatus, id='id'),
            OneToMany(Task, 'statuses', TaskStatus),
            returns=Task,
        )

        print(
            f'Compile: {time.time() - started_at}',
        )

        objects = q.many()

        print(
            f'Mapping: {time.time() - started_at}',
        )

    # == [
    #     Task(id=1, name='First', statuses=[
    #         TaskStatus(id=1, status='CREATED'),
    #         TaskStatus(id=4, status='STARTED'),
    #         TaskStatus(id=5, status='FINISHED'),
    #     ]),
    #     Task(id=2, name='Second', statuses=[
    #         TaskStatus(id=2, status='CREATED'),
    #     ]),
    #     Task(id=3, name='Third', statuses=[
    #         TaskStatus(id=3, status='CREATED'),
    #     ]),
    # ]

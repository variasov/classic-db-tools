import logging.config
from dataclasses import dataclass
from unittest.mock import Mock

from classic.sql_tools import Engine, ToCls, OneToMany
from classic.components import factory
import psycopg


@dataclass
class Task:
    id: int
    name: str
    statuses: list['TaskStatus'] = factory(list)


@dataclass
class TaskStatus:
    id: int
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
        ('task__id', 1),
        ('task__name', 4),
        ('taskstatus__id', 1),
        ('taskstatus__status', 4),
    ]
    cursor.connection = Mock(psycopg.Connection)

    logging.config.dictConfig({
        'version': 1,
        'handlers': {
            '': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
            },
        },
        'loggers': {
            'timer': {
                'level': 'INFO',
                'handlers': [''],
            }
        }
    })

    for index in range(3):
        assert engine.from_str(query).execute(
            cursor
        ).returning(
            ToCls(Task),
            ToCls(TaskStatus),
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

    result = engine.from_str(query).execute(
        cursor
    ).returning(
        ToCls(Task),
        ToCls(TaskStatus),
        OneToMany(Task, 'statuses', TaskStatus),
        returns=Task,
    )
    print(result.mapper_sources)

import os.path
from dataclasses import dataclass
import logging.config
from unittest.mock import Mock
import time

from classic.sql_tools import Engine, ToCls, OneToMany
from classic.components import factory

import psycopg

from conftest import tasks_rows


# App
@dataclass(slots=True)
class Task:
    id: int
    name: str
    statuses: list['TaskStatus'] = factory(list)


@dataclass(slots=True)
class TaskStatus:
    id: int
    status: str


# Mock connection and cursor
cursor = Mock(psycopg.Cursor)
cursor.fetchall = Mock(return_value=tasks_rows(100))
cursor.__module__ = 'psycopg'
cursor.description = [
    ('task__id', 1),
    ('task__name', 4),
    ('taskstatus__id', 1),
    ('taskstatus__status', 4),
]
cursor.connection = Mock(psycopg.Connection)
cursor.connection.__module__ = 'psycopg'


# Logging conf
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


def test_returning_speed():
    print('\n')

    engine = Engine(os.path.join(os.path.dirname(__file__), 'sql'))

    for index in range(10):
        conn = psycopg.connect(
            'user=variasov '
            'password=123 '
            'host=localhost '
            'port=5432 '
            'dbname=test '
        )
        started_at = time.time()

        results = (
            engine
            .from_str('''
                SELECT
                tasks.id           AS Task__id,
                tasks.name         AS Task__name,
                task_status.id     AS TaskStatus__id,
                task_status.status AS TaskStatus__status
                FROM tasks
                JOIN task_status ON task_status.task_id = tasks.id
                LIMIT 5000
            ''')
            .execute(conn)
            .returning(
                ToCls(Task),
                ToCls(TaskStatus),
                OneToMany(Task, 'statuses', TaskStatus),
                returns=Task,
            )
            .many()
         )

        print(f'Finished in {time.time() - started_at}s')
        print(f'Objects mapped: {len(results)}')

        conn.close()

import os
from itertools import cycle, chain, repeat

from classic.db_utils import transaction
from classic.sql_tools import Engine
import psycopg


def tasks_rows(tasks_count: int):
    return list(zip(
        chain.from_iterable(
            repeat(task_id, 3)
            for task_id in range(tasks_count)
        ),
        cycle(
            chain.from_iterable(
                repeat(task_name, 3)
                for task_name in ('First', 'Second', 'Third')
            )
        ),
        range(3 * tasks_count),
        cycle(('CREATED', 'STARTED', 'FINISHED')),
    ))


def test_save_to_db():
    engine = Engine(os.path.join(os.path.dirname(__file__), 'sql'))
    conn = psycopg.connect(
        'user=variasov '
        'password=123 '
        'host=localhost '
        'port=5432 '
        'dbname=test '
    )

    tasks_count = 10000

    with transaction(conn):
        engine.from_str('''
            CREATE TABLE tasks (
                id serial PRIMARY KEY,
                name varchar NULL
            );
            
            CREATE TABLE task_status (
                id serial PRIMARY KEY,
                status varchar NULL,
                task_id int NULL
            )
        ''').execute(conn)

        engine.from_str('''
            INSERT INTO tasks(id, name)
            VALUES ({{ task_id }}, {{ task_name }})
        ''').execute(
            conn, (
                {
                    'task_id': task_id,
                    'task_name': task_name,
                }
                for task_id, task_name in zip(
                    range(tasks_count),
                    cycle(
                        chain.from_iterable(
                            repeat(task_name, 3)
                            for task_name in ('First', 'Second', 'Third')
                        )
                    ),
                )
            )
        )

        engine.from_str('''
            INSERT INTO task_status(id, task_id, status)
            VALUES ({{ status_id }}, {{ task_id }}, {{ task_name }})
        ''').execute(
            conn, (
                {
                    'status_id': status_id,
                    'task_id': task_id,
                    'task_name': task_name,
                }
                for status_id, task_id, task_name in zip(
                    range(3 * tasks_count),
                    chain.from_iterable(
                        repeat(task_id, 3)
                        for task_id in range(tasks_count)
                    ),
                    cycle(('CREATED', 'STARTED', 'FINISHED')),
                )
            )
        )

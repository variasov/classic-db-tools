from datetime import datetime, timedelta
from dataclasses import dataclass

from classic.db_tools import Engine, Mapper, Entity
from classic.criteria import criteria
import pytest


@pytest.fixture
def tasks(engine: Engine, ddl):
    engine.query_from('example/save_task.sql').executemany([
        {'name': 'First', 'value': ''},
        {'name': 'Second', 'value': ''},
        {'name': 'Third', 'value': ''},
        {'name': 'Four', 'value': ''},
    ])
    engine.query('''
    INSERT INTO task_status (title, task_id, created_at)
    VALUES (%(title)s, %(task_id)s, %(created_at)s)
    RETURNING id;
    ''', static=True).executemany([
        {'title': 'CREATED', 'task_id': 1, 'created_at': datetime(2000, 1, 1, 0, 0, 0)},
        {'title': 'STARTED', 'task_id': 1, 'created_at': datetime(2000, 1, 1, 0, 1, 0)},
        {'title': 'FINISHED', 'task_id': 1, 'created_at': datetime(2000, 1, 1, 0, 2, 0)},
        {'title': 'CREATED', 'task_id': 2, 'created_at': datetime(2000, 1, 1, 0, 0, 0)},
        {'title': 'CREATED', 'task_id': 3, 'created_at': datetime(2000, 1, 1, 0, 0, 0)},
    ])
    yield


@dataclass
class Task:
    id: int
    name: str

    @criteria
    def is_finished(self):
        pass

    @criteria
    def older_than(self, period: timedelta):
        pass


mapper = Mapper(task=Entity(Task, 'id'))


def test_render_criteria(engine: Engine, ddl, tasks):
    crit = Task.is_finished() & Task.older_than(
        period=timedelta(microseconds=1),
    )

    objects = engine.query_from(
        'tasks/find.sql.tmpl'
    ).map_to(
        Task, mapper=mapper,
    ).all(criteria=crit)

    assert objects == [Task(1, 'First')]

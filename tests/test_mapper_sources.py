from classic.db_tools import Engine

from .dto import Task


mapper_sources = '''def mapper_func(rows):
    task__id = 0
    task_map = {}
    last_task = None
    for row in rows:
        task_id = (row[task__id],)
        task = task_map.get(task_id)
        if task is None:
            task = task_map[task_id] = Task(id=row[task__id])
            if last_task is not None:
                yield last_task
            last_task = task
    yield last_task'''


def test__mapper__sources(engine: Engine):
    assert engine.query(
        'SELECT 1 AS task__id'
    ).return_as(Task).sources() == mapper_sources
